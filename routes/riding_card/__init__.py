from routes.riding_card.view import (
        GetRidingCardHandle,
        SendRidingCardHandle,
        CurrentDuringTimeHandler,
        AddCountHandler,
        EditRidingCardHandle,
        BusEditRidingCardHandle,
        ClientGetRidingCardHandle,
)

# /ebike_account/riding_card
urls = [
        (r'/internal/get_riding_card', GetRidingCardHandle),
        (r"/internal/edit_riding_card", EditRidingCardHandle),
        (r'/internal/send_riding_card', SendRidingCardHandle),
        (r'/internal/current_during_time', CurrentDuringTimeHandler),
        (r'/internal/add_count', AddCountHandler),

        # B端
        (r"/business/edit_riding_card", BusEditRidingCardHandle),

        # C端
        (r"/client/get_riding_card", ClientGetRidingCardHandle),

]
