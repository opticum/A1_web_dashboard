import requests

# Your bot token (keep this secret!)
BOT_TOKEN = "5914232342:AAFsZlWvaXMEP_MXT6a2nj1XVNVc8_l2VVE"

# Your chat ID
CHAT_ID = "-1003827596073"

# Message to send
message = "Test message from Python 🚀"

# Telegram API URL
url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# Payload
payload = {
    "chat_id": CHAT_ID,
    "text": message
}

# Send request
response = requests.post(url, data=payload)

# Check response
if response.status_code == 200:
    print("Message sent successfully!")
else:
    print("Failed to send message")
    print(response.text)