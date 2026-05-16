const express = require('express');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const multer = require('multer');
const path = require('path');
require('dotenv').config({ path: '../.env' }); // Load shared environment variables

const FASTAPI_URL = process.env.FASTAPI_URL || 'http://localhost:8000';

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'))); // Serve our sleek HTML form

const upload = multer({ storage: multer.memoryStorage() }); // For handling form image uploads

// Initialize whatsapp-web.js client
const client = new Client({
    authStrategy: new LocalAuth()
});

client.on('qr', (qr) => {
    console.log('Scan the QR code below to log in to WhatsApp Web:');
    qrcode.generate(qr, { small: true });
});

client.on('ready', () => {
    console.log('✅ Client is ready! WhatsApp Web is connected.');
});

client.on('auth_failure', msg => {
    console.error('AUTHENTICATION FAILURE', msg);
});

// Listen for all messages (including ones sent from the bot's own phone)
client.on('message_create', async msg => {
    try {
        // If the message was sent BY the bot/plumber's own phone (fromMe = true),
        // we only want to forward it if it's a short command, to avoid infinite loops when the bot replies to itself.
        if (msg.fromMe) {
            const bodyUpper = msg.body.trim().toUpperCase();
            const commands = ["URGENT", "NOT URGENT", "ALL TASKS", "EMERGENCY", "NON EMERGENCY", "NO EMERGENCY", "FILTER", "MID", "ALL"];
            if (!commands.includes(bodyUpper)) {
                return; // Ignore bot's own automated replies and standard outgoing messages
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

        // Helper to swallow known whatsapp-web.js bug where it sends successfully but crashes returning the Message object
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

            // Construct a clean, visually structured text menu
            const menuHeader = text || "⚠️ *Action Required* ⚠️\nPlease select an option by replying with one of the keywords below:";

            // Formats options cleanly, e.g., "👉 *EMERGENCY* - Emergency"
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

        // Forward to the new clean FastAPI endpoint using dynamic URL
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
    console.log(`wbot API server running on https://plumber-backend-fnh6.onrender.com`);
    console.log(`🌍 Customer Web Form is live at: https://plumber-backend-fnh6.onrender.com`);
});
