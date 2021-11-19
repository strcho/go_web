from routes.riding_card.view import (
        GetRidingCardHandle,
        SendRidingCardHandle,
        CurrentDuringTimeHandler,
        AddCountHandler,
        EditRidingCardHandle,
)

# /account/user
urls = [
        (r'/get_riding_card', GetRidingCardHandle),
        (r"/edit_riding_card", EditRidingCardHandle),
        (r'/send_riding_card', SendRidingCardHandle),
        (r'/current_during_time', CurrentDuringTimeHandler),
        (r'/add_count', AddCountHandler),
]
