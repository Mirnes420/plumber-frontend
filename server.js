import express from 'express';
import pkg from 'whatsapp-web.js';
const { Client, LocalAuth, MessageMedia } = pkg;
import qrcode from 'qrcode-terminal';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import puppeteer from 'puppeteer';
import dotenv from 'dotenv';

// Load shared environment variables
dotenv.config({ path: '../.env' });

// ESM equivalent for __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const FASTAPI_URL = process.env.FASTAPI_URL || 'http://localhost:8000';

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'))); // Serve our sleek HTML form

const upload = multer({ storage: multer.memoryStorage() }); // For handling form image uploads

// Launch headless browser instance
const browser = await puppeteer.launch({
    headless: 'new',
    args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu'
    ]
});

const client = new Client({
    authStrategy: new LocalAuth({
        dataPath: process.env.DATA_PATH || './.wwebjs_auth'
    }),
    puppeteer: {
        executablePath: puppeteer.executablePath(),
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox']
    }
});

let currentQR = "";

client.on('qr', (qr) => {
    console.log('Scan the QR code below to log in to WhatsApp Web:');
    qrcode.generate(qr, { small: true });
    currentQR = qr; // Save for web frontend
});

client.on('ready', () => {
    currentQR = "";
    console.log('✅ Client is ready! WhatsApp Web is connected.');
});

client.on('auth_failure', msg => {
    console.error('AUTHENTICATION FAILURE', msg);
});

// Web interface to scan the QR Code from the cloud!
app.get('/auth', (req, res) => {
    if (client.info && client.info.pushname) {
        return res.send('<h2 style="font-family:sans-serif; text-align:center; margin-top:50px; color:green;">✅ WhatsApp is already connected!</h2>');
    }
    if (!currentQR) {
        return res.send('<h2 style="font-family:sans-serif; text-align:center; margin-top:50px;">⌛ Generating QR code... Please refresh this page in a few seconds.</h2>');
    }
    
    res.send(`
        <html>
            <head>
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
                <style>
                    body { display:flex; justify-content:center; align-items:center; height:100vh; background:#0f172a; margin:0; font-family:sans-serif; color:white; }
                    .card { text-align:center; padding: 2rem; background:rgba(30,41,59,0.9); border-radius:16px; border:1px solid rgba(255,255,255,0.1); }
                    #qrcode { margin: 20px auto; background: white; padding: 10px; border-radius: 8px; display:inline-block; }
                </style>
            </head>
            <body>
                <div class="card">
                    <h2>Link your WhatsApp</h2>
                    <p>Scan this QR code with your WhatsApp app.</p>
                    <div id="qrcode"></div>
                    <p style="color:#94a3b8; font-size:14px;">If it doesn't work, refresh the page to get a new code.</p>
                </div>
                <script>
                    new QRCode(document.getElementById("qrcode"), {
                        text: "${currentQR}",
                        width: 256,
                        height: 256
                    });
                    
                    // Auto-refresh to check if connected
                    setInterval(() => {
                        window.location.reload();
                    }, 15000);
                </script>
            </body>
        </html>
    `);
});

// Listen for all messages (including ones sent from the bot's own phone)
client.on('message_create', async msg => {
    try {
        if (msg.fromMe) {
            const bodyUpper = msg.body.trim().toUpperCase();
            const commands = ["URGENT", "NOT URGENT", "ALL TASKS", "EMERGENCY", "NON EMERGENCY", "NO EMERGENCY", "FILTER", "MID", "ALL"];
            if (!commands.includes(bodyUpper)) {
                return;
            }
        }

        console.log(`Received message from ${msg.from}: ${msg.body}`);

        const payload = {
            From: msg.from.replace("@c.us", "").replace("@g.us", ""),
            Body: msg.body
        };

        if (msg.hasMedia) {
            payload.MediaUrl0 = "media_attached_but_unsupported_by_simple_forwarder";
        }
        console.log('calling the webhook endpoint from the fastapi')
        await fetch(`${FASTAPI_URL}/webhook`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
    } catch (error) {
        console.error(`Error forwarding to webhook. Is FastAPI running on ${FASTAPI_URL}?`, error.message);
    }
});

client.initialize();

// API Endpoint to send messages
app.post('/send', async (req, res) => {
    try {
        const { number, text, imageUrl, caption, buttons } = req.body;

        if (!number) {
            return res.status(400).json({ error: 'Number is required (e.g. 385919293138)' });
        }

        const chatId = number + "@c.us";

        const safeSend = async (id, content, options = {}) => {
            try {
                await client.sendMessage(id, content, options);
            } catch (e) {
                if (e.message && e.message.includes("getChat")) {
                    console.log(`⚠️ Ignored known library bug (Message ACTUALLY SENT!): ${e.message}`);
                } else {
                    throw e;
                }
            }
        };

        if (buttons && buttons.length > 0) {
            console.log(`Sending keyword-optimized text menu to ${chatId}`);

            const menuHeader = text || "⚠️ *Action Required* ⚠️\nPlease select an option by replying with one of the keywords below:";
            const menuBody = buttons.map(b => `👉 *${b.toUpperCase().trim()}*`).join('\n');
            const fullMenuText = `${menuHeader}\n\n${menuBody}\n\n_Type your chosen keyword exactly as shown to respond._`;

            await safeSend(chatId, fullMenuText);
            console.log("✅ Keyword text menu sent successfully");
        } else if (imageUrl) {
            console.log(`Sending image to ${chatId}`);
            try {
                const media = await MessageMedia.fromUrl(imageUrl, { unsafeMime: true });
                await safeSend(chatId, media, { caption: caption || text || "" });
                console.log("✅ Image sent successfully");
            } catch (mediaError) {
                if (mediaError.message && mediaError.message.includes("getChat")) {
                    console.log("⚠️ Ignored getChat error for image");
                } else {
                    key
                    console.error("MessageMedia failed:", mediaError.message);
                    console.log("Falling back to text-only with image URL");
                    const fallbackText = `${caption || text || ""}\n\nImage Attachment: ${imageUrl}`;
                    await safeSend(chatId, fallbackText);
                }
            }
        } else if (text) {
            console.log(`Sending text to ${chatId}`);
            await safeSend(chatId, text);
        } else {
            return res.status(400).json({ error: 'Either text, imageUrl, or buttons is required' });
        }

        res.json({ success: true, message: 'Message sent!' });
    } catch (error) {
        console.error("Error sending message:", error);
        res.status(500).json({ error: error.message });
    }
});

// Endpoint to receive HTML form submissions and forward to FastAPI
app.post('/submit-form', upload.single('image'), async (req, res) => {
    try {
        const { phone, description } = req.body;
        console.log(`🌐 Received web form from ${phone}`);

        const formData = new FormData();
        formData.append('phone', phone);
        formData.append('description', description);

        if (req.file) {
            const blob = new Blob([req.file.buffer], { type: req.file.mimetype });
            formData.append('image', blob, req.file.originalname);
        }
        console.log('calling the incident endpoint from the fastapi')
        const response = await fetch(`${FASTAPI_URL}/api/incident`, {
            method: 'POST',
            body: formData
        });

        const result = await response.json();

        if (!response.ok) {
            console.error("FastAPI Error Response:", JSON.stringify(result, null, 2));
            throw new Error(result.detail ? JSON.stringify(result.detail) : (result.message || 'FastAPI rejected the request'));
        }

        res.json({ success: true, result });
    } catch (error) {
        console.error("Web form processing error:", error);
        res.status(500).json({ error: error.message });
    }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`wbot API server running on port ${PORT}`);
});