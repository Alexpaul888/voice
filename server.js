require('dotenv').config();
const express = require('express');
const app = express();
const expressWs = require('express-ws')(app);

const axios = require('axios');
const FormData = require('form-data');
const { WaveFile } = require('wavefile');

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Sockets (Calls) ke reference variables
let agentWs = null;
let customerWs = null;
let agentStreamSid = null;
let customerStreamSid = null;

// Voice Activity Detection (VAD) Settings
const SILENCE_THRESHOLD = 400; 
const MAX_SILENCE_FRAMES = 25; 
let isSpeaking = false;
let silenceFrames = 0;
let agentVoiceBuffer = [];

// Mu-Law aawaz ko PCM mein badalne ka formula
const muLawToPcm = new Int16Array(256);
for (let i = 0; i < 256; i++) {
    let mu = ~i;
    let sign = (mu & 0x80) ? -1 : 1;
    let exponent = (mu >> 4) & 0x07;
    let mantissa = mu & 0x0F;
    muLawToPcm[i] = sign * (((mantissa << 3) + 0x84) << exponent) - 0x84;
}

// ----------------------------------------------------
// ElevenLabs API (Voice Changer)
// ----------------------------------------------------
async function convertVoice(audioBufferArray) {
    if (!customerWs || customerWs.readyState !== 1) {
        console.log("Customer abhi line par nahi hai, aawaz nahi bheji.");
        return;
    }
    
    try {
        const rawAudio = Buffer.concat(audioBufferArray);
        const wav = new WaveFile();
        wav.fromScratch(1, 8000, '8m', rawAudio);
        const wavBuffer = wav.toBuffer();

        const form = new FormData();
        form.append('audio', wavBuffer, { filename: 'audio.wav', contentType: 'audio/wav' });

        console.log("-> ElevenLabs ko aawaz bhej rahe hain...");
        
        const response = await axios.post(
            `https://api.elevenlabs.io/v1/speech-to-speech/${process.env.ELEVENLABS_VOICE_ID}/stream?output_format=ulaw_8000`,
            form,
            {
                headers: { ...form.getHeaders(), 'xi-api-key': process.env.ELEVENLABS_API_KEY },
                responseType: 'stream'
            }
        );

        response.data.on('data', (chunk) => {
            if (customerWs && customerWs.readyState === 1 && customerStreamSid) {
                customerWs.send(JSON.stringify({
                    event: "media",
                    streamSid: customerStreamSid,
                    media: { payload: chunk.toString('base64') }
                }));
            }
        });
        
        response.data.on('end', () => console.log("<- ElevenLabs ne aawaz bhej di."));

    } catch (err) {
        console.error("X ElevenLabs Error:", err.response ? err.response.data : err.message);
    }
}

// ----------------------------------------------------
// VOBIZ WEBHOOKS
// ----------------------------------------------------
app.post('/vobiz-inbound-agent', (req, res) => {
    console.log(`\n-> 📞 Agent Webhook Hit! Event: ${req.body.Event || 'Start'} | Status: ${req.body.CallStatus}`);
    res.set('Content-Type', 'text/xml');

    if (req.body.Event === 'Hangup' || req.body.CallStatus === 'completed' || req.body.CallStatus === 'hangup') {
        return res.send('<Response/>');
    }

    const wsUrl = process.env.BASE_URL.replace(/^http/, 'ws') + '/agent-stream';
    // Fix: Connect hata diya aur 300 seconds (5 min) ka Pause laga diya taaki call na kate
    const twiml = `<Response><Stream url="${wsUrl}" /><Pause length="300"/></Response>`;
    res.send(twiml);
});

app.post('/vobiz-inbound-customer', (req, res) => {
    console.log(`\n-> 📞 Customer Webhook Hit! Event: ${req.body.Event || 'Start'} | Status: ${req.body.CallStatus}`);
    
    // Ngrok Warning Bypass & Standard Headers
    res.set({
        'Content-Type': 'text/xml',
        'ngrok-skip-browser-warning': 'true'
    });
    
    if (req.body.Event === 'Hangup' || req.body.CallStatus === 'completed' || req.body.CallStatus === 'hangup') {
        console.log("-> Call Hangup event handled.");
        return res.send('<Response/>');
    }

    const wsUrl = process.env.BASE_URL.replace(/^http/, 'ws') + '/customer-stream';
    const twiml = `<Response><Stream url="${wsUrl}" /><Pause length="300"/></Response>`;
    
    console.log("-> Vobiz ko bhej rahe hain XML:", twiml);
    res.send(twiml);
});

// Outbound Call Lagane ka API
app.post('/make-call', async (req, res) => {
    const { to } = req.body;
    try {
        console.log(`=> Vobiz ko bol rahe hain ${to} par call lagane ke liye...`);

        const authId = process.env.VOBIZ_AUTH_ID;
        const authSecret = process.env.VOBIZ_AUTH_SECRET;
        const url = `https://api.vobiz.ai/api/v1/Account/${authId}/Call/`;

        const response = await axios.post(url, {
            to: to,
            from: process.env.VOBIZ_DID,
            answer_url: `${process.env.BASE_URL}/vobiz-inbound-customer`
        }, {
            headers: { 
                'X-Auth-ID': authId,
                'X-Auth-Token': authSecret,
                'Content-Type': 'application/json'
            }
        });

        console.log("=> Call lag gayi! Response:", response.data);
        res.json({ success: true, response: response.data });
    } catch (err) {
        console.log("!!! VOBIZ API NE CALL REJECT KAR DI !!!");
        console.error("Asli Kaaran (Reason):", err.response ? JSON.stringify(err.response.data, null, 2) : err.message);
        res.status(500).json({ error: "Call lagane mein dikkat aayi" });
    }
});

// ----------------------------------------------------
// WEBSOCKETS
// ----------------------------------------------------
app.ws('/agent-stream', (ws) => {
    agentWs = ws;
    console.log("-> 🟢 Agent WebSocket Connect ho gaya!");

    ws.on('message', (msg) => {
        try {
            const data = JSON.parse(msg);
            if (data.event === "start") {
                agentStreamSid = data.streamSid;
                console.log("-> Agent Stream Start SID:", agentStreamSid);
            }
        } catch (e) { console.error("Agent WS Error:", e); }
    });

    ws.on('close', () => { agentWs = null; console.log("-> ❌ Agent WebSocket Disconnect hua."); });
    ws.on('error', (err) => { console.error("-> ⚠️ Agent WS Error:", err); });
});

app.ws('/customer-stream', (ws, req) => {
    console.log("-> 🔥🔥🔥 BINGO! Customer WebSocket Connection Request Aagayi! 🔥🔥🔥");
    customerWs = ws;
    console.log("-> 🟢 Customer WebSocket Connect ho gaya!");

    ws.on('message', (msg) => {
        try {
            const data = JSON.parse(msg);
            if (data.event === "start") {
                customerStreamSid = data.streamSid;
                console.log("-> Customer Stream Start SID:", customerStreamSid);
            }
            if (data.event === "media") {
                if (agentWs && agentWs.readyState === 1 && agentStreamSid) {
                    agentWs.send(JSON.stringify({
                        event: "media",
                        streamSid: agentStreamSid,
                        media: { payload: data.media.payload }
                    }));
                }
            }
        } catch (e) { console.error("Customer WS Error:", e); }
    });

    ws.on('close', () => { customerWs = null; console.log("-> ❌ Customer WebSocket Disconnect ho gaya."); });
    ws.on('error', (err) => { console.error("-> ⚠️ Customer WS Error:", err); });
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server started on port ${PORT}`);
});