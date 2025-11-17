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
        self.daily_remaining = 50

    def send_request(self, subject):
        url = self.url_base + subject
        response = requests.get(url, headers=self.headers)
        response_headers = response.headers

        self.daily_remaining = int(response_headers.get('X-RateLimit-Requests-Remaining'))
        self.daily_limit = int(response_headers.get('X-RateLimit-Requests-Limit'))
        print(f"API Quota: You have {self.daily_remaining} / {self.daily_limit} calls remaining.")

        return response

    def get_actual_rankings(self):
        if self.daily_remaining > 0:
            rankings_raw = self.send_request("rankings/atp/live")
            rankings_json = rankings_raw.json()

            self.database.fill_ranking(rankings_json)

            file = f"rank/rankings_{date.today().strftime('%d%m%Y')}.json"
            with open(file, 'w') as f:
                json.dump(rankings_json, f, indent=4)
                print("file saved")

    def load_player_info(self):
        if self.daily_remaining > 0:
            if self.database.find_players_missing_info():
                player_id = self.database.find_players_missing_info()[0][0]
                print(player_id)
                player_raw = self.send_request(f"player/{player_id}")
                player_json = player_raw.json()
                self.database.add_additional_player_info(player_json)
                file = f"player/{player_id}_{date.today().strftime('%d%m%Y')}.json"
                with open(file, 'w') as f:
                    json.dump(player_json, f, indent=4)
                    print("file saved")
                return True
            else:
                return False

    def load_player_events(self, history):
        if self.database.find_player_for_match_backfill(history):
            player_id, name, page = self.database.find_player_for_match_backfill(history)
            events_raw = self.send_request(f"player/{player_id}/events/previous/{page}")
            events_json = events_raw.json()
            self.database.add_match(events_json)
            self.database.change_match_player_suffix(player_id, history+1)

            file = f"events/{player_id}_{date.today().strftime('%d%m%Y')}.json"
            with open(file, 'w') as f:
                json.dump(events_json, f, indent=4)
                print("file saved")
            return True
        else:
            if self.database.find_recent_unfinished_matches(20):
                player_id = self.database.find_recent_unfinished_matches(20)[0][0]
                events_raw = self.send_request(f"player/{player_id}/events/previous/0")
                events_json = events_raw.json()
                self.database.add_match(events_json)

                file = f"events/{player_id}_{date.today().strftime('%d%m%Y')}.json"
                with open(file, 'w') as f:
                    json.dump(events_json, f, indent=4)
                    print("file saved")
                return True
            else:
                return False


if __name__ == "__main__":
    API = API_requests()
    API.get_actual_rankings()
    proceeded = True
    while(API.daily_remaining > 0 and proceeded):
        proceeded = API.load_player_info()
    proceeded = True
    while(API.daily_remaining > 0 and proceeded):
        API.load_player_events(0)

#url = "https://tennisapi1.p.rapidapi.com/api/tennis/rankings/atp/live"
#url = "https://tennisapi1.p.rapidapi.com/api/tennis/player/275923/events/near"
#url = "https://tennisapi1.p.rapidapi.com/api/tennis/team/275923/events/previous/0"

#data = response.json()

#file_name = f"rank/alcarazevents_{date.today().strftime('%d%m%Y')}.json"