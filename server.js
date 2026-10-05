require('dotenv').config();
const express = require('express');
const app = express();
const expressWs = require('express-ws')(app);

const axios = require('axios');
const FormData = require('form-data');
const { WaveFile } = require('wavefile');

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

let agentWs = null;
let customerWs = null;
let agentStreamSid = null;
let customerStreamSid = null;

// Helper function to build Vobiz WebSocket URL
function getWsUrl(endpoint) {
    const baseUrl = process.env.BASE_URL || 'https://localhost:3000';
    return baseUrl.replace(/^http/, 'ws') + endpoint;
}

// ----------------------------------------------------
// ElevenLabs API (Voice Changer)
// ----------------------------------------------------
async function convertVoice(audioBufferArray) {
    if (!customerWs || customerWs.readyState !== 1) {
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
                    event: "playAudio",
                    streamId: customerStreamSid,
                    media: { 
                        contentType: "audio/x-mulaw",
                        sampleRate: 8000,
                        payload: chunk.toString('base64') 
                    }
                }));
            }
        });
        
        response.data.on('end', () => console.log("<- ElevenLabs ne aawaz bhej di."));

    } catch (err) {
        console.error("X ElevenLabs Error:", err.response ? err.response.data : err.message);
    }
}

// ----------------------------------------------------
// VOBIZ WEBHOOKS (Official Vobiz XML Format)
// ----------------------------------------------------
app.post('/vobiz-inbound-agent', (req, res) => {
    console.log(`\n-> 📞 Agent Webhook Hit! Event: ${req.body.Event || 'Start'}`);
    res.type('text/xml');

    if (req.body.Event === 'Hangup' || req.body.CallStatus === 'completed' || req.body.CallStatus === 'hangup') {
        return res.send('<?xml version="1.0" encoding="UTF-8"?><Response/>');
    }

    const wsUrl = getWsUrl('/agent-stream');
    const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Stream bidirectional="true">${wsUrl}</Stream>
</Response>`;

    res.send(twiml);
});

app.post('/vobiz-inbound-customer', (req, res) => {
    console.log(`\n-> 📞 Customer Webhook Hit! Event: ${req.body.Event || 'Start'}`);
    res.type('text/xml');
    
    if (req.body.Event === 'Hangup' || req.body.CallStatus === 'completed' || req.body.CallStatus === 'hangup') {
        console.log("-> Call Hangup event handled.");
        return res.send('<?xml version="1.0" encoding="UTF-8"?><Response/>');
    }

    const wsUrl = getWsUrl('/customer-stream');
    const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Stream bidirectional="true">${wsUrl}</Stream>
</Response>`;
    
    console.log("-> Vobiz ko bhej rahe hain Official XML:\n", twiml);
    res.send(twiml);
});

// Outbound Call API
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
        console.error("Reason:", err.response ? JSON.stringify(err.response.data, null, 2) : err.message);
        res.status(500).json({ error: "Call lagane mein dikkat aayi" });
    }
});

// ----------------------------------------------------
// WEBSOCKETS (Vobiz Stream Event Format)
// ----------------------------------------------------
app.ws('/agent-stream', (ws) => {
    agentWs = ws;
    console.log("-> 🟢 Agent WebSocket Connect ho gaya!");

    ws.on('message', (msg) => {
        try {
            const data = JSON.parse(msg);
            if (data.event === "start") {
                agentStreamSid = data.start?.streamId || data.streamSid;
                console.log("-> Agent Stream Start ID:", agentStreamSid);
            }
        } catch (e) { console.error("Agent WS Error:", e); }
    });

    ws.on('close', () => { agentWs = null; console.log("-> ❌ Agent WebSocket Disconnect."); });
    ws.on('error', (err) => { console.error("-> ⚠️ Agent WS Error:", err); });
});

app.ws('/customer-stream', (ws, req) => {
    console.log("-> 🔥🔥🔥 BINGO! Customer WebSocket Connection Request Aagayi! 🔥🔥🔥");
    customerWs = ws;

    ws.on('message', (msg) => {
        try {
            const data = JSON.parse(msg);
            if (data.event === "start") {
                customerStreamSid = data.start?.streamId || data.streamSid;
                console.log("-> Customer Stream Start ID:", customerStreamSid);
            }
        } catch (e) { console.error("Customer WS Error:", e); }
    });

    ws.on('close', () => { customerWs = null; console.log("-> ❌ Customer WebSocket Disconnect."); });
    ws.on('error', (err) => { console.error("-> ⚠️ Customer WS Error:", err); });
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server started on port ${PORT}`);
});