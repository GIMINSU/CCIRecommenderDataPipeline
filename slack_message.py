from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from config import SlackConfig

# ID of channel you senwant to post message to

def send_simple_message(message):
    
    # WebClient instantiates a client that can call API methods
    # When using Bolt, you can use either `app.client` or the `client` passed to listeners.
    client = WebClient(token=SlackConfig.TOKEN)
    channel_id = SlackConfig.CHANNEL_ID

    try:
        # Call the conversations.list method using the WebClient
        result = client.chat_postMessage(
            channel=channel_id,
            text=message
            # You could also use a blocks[] array to send richer content
        )
        # Print result, which includes information about the message (like TS)
        print(result)

    except SlackApiError as e:
        print(f"Error: {e}")