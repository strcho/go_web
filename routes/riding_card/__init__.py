from routes.riding_card.view import (
        PlatformRidingCardHandle,
        SendRidingCardHandle,
        CurrentDuringTimeHandler,
        AddCountHandler,
)

# /account/user
urls = [
        (r'/riding_card', PlatformRidingCardHandle),
        (r'/send_riding_card', SendRidingCardHandle),
        (r'/current_during_time', CurrentDuringTimeHandler),
        (r'/add_count', AddCountHandler),
]
