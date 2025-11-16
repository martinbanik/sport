import requests
import json
from datetime import date
from database import Database


class API_requests:
    def __init__(self):
        self.headers = {
	        "x-rapidapi-key": "65d1e80dddmsh52c6c55ea4a8191p1f22e2jsn376a1f3c23e8",
	        "x-rapidapi-host": "tennisapi1.p.rapidapi.com"
        }
        self.url_base = "https://tennisapi1.p.rapidapi.com/api/tennis/"
        self.database = Database("tennis.db")

    def send_request(self, subject):
        url = self.url_base + subject
        response = requests.get(url, headers=self.headers)
        response_headers = response.headers

        self.daily_remaining = response_headers.get('X-RateLimit-Requests-Remaining')
        self.daily_limit = response_headers.get('X-RateLimit-Requests-Limit')
        print(f"API Quota: You have {self.daily_remaining} / {self.daily_limit} calls remaining.")

        return response

    def get_actual_rankings(self):
        rankings_raw = self.send_request("rankings/atp/live")
        rankings_json = rankings_raw.json()
        file = f"rank/rankings_{date.today().strftime('%d%m%Y')}.json"
        with open(file, 'w') as f:
            json.dump(rankings_json, f, indent=4)
            print("file saved")

    def load_player_info(self):
        player_id = self.database.crosstableSearch("player_id", "rank", 5, "Rankings")
        player_raw = self.send_request(f"player/{player_id}")

if __name__ == "__main__":
    API = API_requests()
    API.get_actual_rankings()



#url = "https://tennisapi1.p.rapidapi.com/api/tennis/rankings/atp/live"
#url = "https://tennisapi1.p.rapidapi.com/api/tennis/player/275923/events/near"
#url = "https://tennisapi1.p.rapidapi.com/api/tennis/team/275923/events/previous/0"

#data = response.json()

#file_name = f"rank/alcarazevents_{date.today().strftime('%d%m%Y')}.json"