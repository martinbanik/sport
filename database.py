import sqlite3
import json
from datetime import date, datetime

class Database:
    def __init__(self, name):
        self.name = name

    def make_new_table_players(self):

        # This creates the file 'se;f.name' if it doesn't exist
        with sqlite3.connect(self.name) as conn:
            cursor = conn.cursor()

            # --- Create the Players table ---
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Players (
                player_id INTEGER PRIMARY KEY,
                name TEXT,
                country TEXT,
                gender TEXT,
                residence TEXT,
                height float,
                weight float,
                plays TEXT,
                turnedPro TEXT,
                prizeTotal INTEGER,
                birth DATETIME,
                update DATETIME
            );
            ''')
            conn.commit()  # Save the changes

    def make_new_table_matches(self):
                # This creates the file 'se;f.name' if it doesn't exist
        with sqlite3.connect(self.name) as conn:
            cursor = conn.cursor()

            # --- Create the Matches table ---
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Matches (
                match_id INTEGER PRIMARY KEY,
                tournament_id INTEGER,
                ground_type TEXT,
                round TEXT,
                match_date DATETIME,
                winner_id INTEGER,
                loser_id INTEGER,
                score TEXT,
                has_stats BIT,
                FOREIGN KEY (winner_id) REFERENCES Players (player_id),
                FOREIGN KEY (loser_id) REFERENCES Players (player_id)
            );
            ''')
            conn.commit()  # Save the changes

    def make_new_table_rankings(self):
                # This creates the file 'se;f.name' if it doesn't exist
        with sqlite3.connect(self.name) as conn:
            cursor = conn.cursor()

            # --- Create the Rankings table ---
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Rankings (
                ranking_id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER,
                ranking_date INTEGER,
                rank INTEGER,
                points INTEGER,
                FOREIGN KEY (player_id) REFERENCES Players (player_id)
            );
            ''')

            print("Database and tables created successfully.")
            conn.commit()  # Save the changes

    def update_players_from_ranking(self):
        with sqlite3.connect(self.name) as conn, open("rank/rankings_10112025.json", "r") as f:
            cursor = conn.cursor()
            data = json.load(f)
            ranking = data['rankings']
            for team in ranking:
                player = team['team']
                print(player['name'])
                try:
                    # Insert players (OR IGNORE stops it from crashing if the player already exists)
                    cursor.execute('''INSERT OR IGNORE INTO Players (
                    player_id, 
                    name, 
                    country) 
                    VALUES (?, ?, ?)''',
                    (player['id'],
                    player['name'],
                    player['country']['name']))
                except sqlite3.IntegrityError:
                    print(f"Player ID {player['id']} already exists in the database.")
            conn.commit()

    def fill_ranking(self, data):
        with sqlite3.connect(self.name) as conn: #, open("rank/rankings_13112025.json", "r") as f:
            cursor = conn.cursor()
            #data = json.load(f)
            ranking = data['rankings']
            for team in ranking:
                try:
                    # Insert players (OR IGNORE stops it from crashing if the player already exists)
                    cursor.execute('''INSERT OR IGNORE INTO Rankings (
                        player_id, 
                        ranking_date, 
                        rank, points) 
                        VALUES (?, ?, ?, ?)''',
                        (team['team']['id'],
                        date.today().strftime('%d%m%Y'),
                        team['ranking'],
                        team['points']))
                except sqlite3.IntegrityError:
                    print(f"Match ID {team['team']['id']} already exists in the database.")
            conn.commit()

    def add_match(self, match_data):
        with (sqlite3.connect(self.name) as conn): #, open("rank/alcarazevents_10112025.json", "r") as f):
            cursor = conn.cursor()
            #match_data = json.load(f)
            for event in match_data['events']:
                if event['status'].get('type') == "finished":
                    print("finished")
                    if event.get('winnerCode') == 1:
                        winner_id = event['homeTeam'].get('id')
                        loser_id = event['awayTeam'].get('id')
                        score = f"{event['homeScore'].get('current')}:{event['awayScore'].get('current')} ("
                        for res in event['homeScore']:
                            if res.startswith("period"):
                                score += f"{event['homeScore'].get(res)}-{event['awayScore'].get(res)}, "
                        score = score[:-2] + ")"
                    else:
                        loser_id = event['homeTeam'].get('id')
                        winner_id = event['awayTeam'].get('id')
                        score = f"{event['awayScore'].get('current')}:{event['homeScore'].get('current')} ("
                        for res in event['homeScore']:
                            if res.startswith("period"):
                                score += f"{event['awayScore'].get(res)}-{event['homeScore'].get(res)}, "
                        score = score[:-2] + ")"
                    tournament_id = event['tournament'].get('id')
                    groundType = event['groundType']
                    date = datetime.fromtimestamp(event.get('startTimestamp'))
                    round = None
                    if event.get('roundInfo'):
                        round = event['roundInfo'].get('slug')
                    try:
                            cursor.execute(
                                '''INSERT OR IGNORE INTO Matches (
                                match_id,
                                tournament_id, 
                                ground_type, 
                                match_date,
                                winner_id,
                                loser_id,
                                round,
                                score) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                                (event.get('id'),
                                tournament_id,
                                groundType, date,
                                winner_id,
                                loser_id,
                                round,
                                score))
                    except sqlite3.Error as e:
                        print(f"An error occurred: {e}")
            conn.commit()

    def add_additional_player_info(self, player_data):
        with (sqlite3.connect(self.name) as conn): #, open("rank/alcarazinfo.json", "r", encoding='utf-8') as f):
            cursor = conn.cursor()
            #player_data = json.load(f)
            player = player_data['team']
            try:
                cursor.execute('''UPDATE Players
                                SET gender = ?, residence = ?, height = ?, weight = ?,
                                plays = ?, turnedPro = ?, prizeTotal = ?, birth = ?
                                WHERE player_id = ?''',
                               (player['gender'],
                                player['playerTeamInfo'].get('residence'),
                                player['playerTeamInfo'].get('height'),
                                player['playerTeamInfo'].get('weight'),
                                player['playerTeamInfo'].get('plays'),
                                player['playerTeamInfo'].get('turnedPro'),
                                player['playerTeamInfo'].get('prizeTotal'),
                                datetime.fromtimestamp(player['playerTeamInfo'].get('birthDateTimestamp')),
                                player['id']))
            except sqlite3.Error as e:
                print(f"An error occurred: {e}")
            conn.commit()

    def update_players_table(self):
        with sqlite3.connect(self.name) as conn:
            cursor = conn.cursor()

            try:
                cursor.execute("UPDATE Players SET gender = 'M'")
            except sqlite3.Error as e:
                print(f"An error occurred: {e}")
            conn.commit()

    def show_stats(self):
        with open("rank/stats_10400727.json", "r") as f:
            data = json.load(f)
            for row in data['statistics']:
                print(f"{row['period']} period")
                print("               1st player------------------------2nd player")
                for group in row['groups']:
                    print(group['groupName'])
                    for stat in group['statisticsItems']:
                        print(f"{stat['name']}:  {stat['home']}     |     {stat['away']}")

    def search(self, table_name, select_cols=['*'], where_conditions={}, fetchone=False):
        """
        Universally search the database with dynamic WHERE clauses.

        :param table_name: The name of the table to search (e.g., 'Players')
        :param select_cols: A list of columns to return (e.g., ['name', 'player_id'])
        :param where_conditions: A dict of WHERE clauses (e.g., {'name': 'Carlos Alcaraz', 'rank': 1})
        :param fetchone: If True, returns a single row. If False, returns all matching rows.
        :return: A list of tuples (rows) or a single tuple (row)
        """

        # --- 1. Security Validation (CRITICAL) ---
        # Define all allowed table and column names to prevent SQL injection.
        # YOU MUST ADD ALL YOUR COLUMN NAMES HERE
        allowed_tables = {'Players', 'Matches', 'Rankings'}
        allowed_cols = {
            '*', 'player_id', 'name', 'country', 'gender', 'residence', 'height',
            'weight', 'plays', 'turnedPro', 'prizeTotal', 'birth', 'info_fetched',
            'match_id', 'tournament_id', 'ground_type', 'round', 'match_date',
            'winner_id', 'loser_id', 'score', 'stats_fetched', 'odds_fetched',
            'ranking_id', 'ranking_date', 'rank', 'points'
        }

        # Validate table name
        if table_name not in allowed_tables:
            print(f"Error: Invalid table name '{table_name}'")
            return None

        # Validate selected columns
        safe_select_cols = []
        for col in select_cols:
            if col not in allowed_cols:
                print(f"Error: Invalid select column '{col}'")
                return None
            safe_select_cols.append(col)

        select_str = ", ".join(safe_select_cols)

        # --- 2. Dynamically Build the WHERE Clause ---
        where_clauses = []
        values = []

        for key, val in where_conditions.items():
            if key not in allowed_cols:
                print(f"Error: Invalid where column '{key}'")
                return None
            where_clauses.append(f"{key} = ?")  # e.g., "name = ?"
            values.append(val)

        # --- 3. Assemble the Final Query ---
        query = f"SELECT {select_str} FROM {table_name}"

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
            # e.g., "SELECT * FROM Players WHERE name = ? AND country = ?"

        # Convert values list to a tuple for execute()
        values_tuple = tuple(values)

        # --- 4. Execute the Query ---
        with sqlite3.connect(self.name) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, values_tuple)
                if fetchone:
                    return cursor.fetchone()  # Returns one row (e.g., (123, 'Carlos'))
                else:
                    return cursor.fetchall()  # Returns a list of rows [(...), (...)]
            except sqlite3.Error as e:
                print(f"An error occurred: {e}")
                return None

if __name__ == "__main__":
    database = Database("tennis.db")
    #database.make_new_table_matches()
    #database.make_new_table_players()
    #database.make_new_table_rankings()
    #database.update_players_from_ranking()
    #database.fill_ranking()
    #database.add_match("dd")
    #database.show_stats()
    #print(database.player_from_rank(1))
    #print(database.player_name_from_id(database.player_from_rank(1)))
    print(database.search('Players',
                         select_cols=['player_id'],
                         where_conditions={'name': 'Carlos Alcaraz'},
                         fetchone=True))
    print(database.search('Players'))
    #database.add_additional_player_info()