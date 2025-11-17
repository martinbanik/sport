import sqlite3
import json
from datetime import date, datetime
import pandas as pd

class Database:
    def __init__(self, name):
        self.name = name
        self.init_tables()

    def init_tables(self):

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
                history_fetched INTEGER DEFAULT 0,
                last_update DATETIME
            );
            ''')
            conn.commit()  # Save the changes

            # --- Create the Matches table ---
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Matches (
                match_id INTEGER PRIMARY KEY,
                tournament_id INTEGER,
                ground_type TEXT,
                round TEXT,
                match_date DATETIME,
                home_player_id INTEGER,
                away_player_id INTEGER,
                score TEXT,
                winner_id INTEGER,
                have_stats BIT DEFAULT 0,
                FOREIGN KEY (home_player_id) REFERENCES Players (player_id),
                FOREIGN KEY (away_player_id) REFERENCES Players (player_id),
                FOREIGN KEY (winner_id) REFERENCES Players (player_id)
            );
            ''')
            conn.commit()  # Save the changes

            # --- Create the Matches table ---
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS MatchStats (
                stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER, 
                period TEXT,
                stat_group TEXT,
                stat_name TEXT,
                home_value TEXT,
                away_value TEXT,
                FOREIGN KEY (match_id) REFERENCES Matches (match_id)
                
                UNIQUE(match_id, period, stat_group, stat_name)
            );
            ''')
            conn.commit()  # Save the changes

            # --- Create the Matches table ---
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Tournaments (
                tournament_id INTEGER PRIMARY KEY,
                name TEXT,
                groundType TEXT,
                tennisPoints INTEGER,
                category TEXT,
                titleHolder TEXT,
                FOREIGN KEY (tournament_id) REFERENCES Matches (tournament_id)
            );
            ''')
            conn.commit()  # Save the changes

            # --- Create the Rankings table ---
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Rankings (
                ranking_id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER,
                ranking_date DATETIME,
                rank INTEGER,
                points INTEGER,
                FOREIGN KEY (player_id) REFERENCES Players (player_id)
            );
            ''')

            print("Database and tables created successfully.")
            conn.commit()  # Save the changes

    def update_players_from_ranking(self):
        with sqlite3.connect(self.name) as conn:#, open("rank/rankings_10112025.json", "r") as f:
            cursor = conn.cursor()
            #data = json.load(f)
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
            date = datetime.fromtimestamp(data['updatedAtTimestamp'])
            for team in ranking:
                try:
                    # Insert players (OR IGNORE stops it from crashing if the player already exists)
                    cursor.execute('''INSERT OR IGNORE INTO Rankings (
                        player_id, 
                        ranking_date, 
                        rank, points) 
                        VALUES (?, ?, ?, ?)''',
                        (team['team']['id'],
                        date,
                        team['ranking'],
                        team['points']))
                except sqlite3.IntegrityError:
                    print(f"Match ID {team['team']['id']} already exists in the database.")
            conn.commit()

    def add_match(self, match_data):
        with (sqlite3.connect(self.name) as conn):#, open("rank/alcarazevents_10112025.json", "r", encoding='utf-8') as f):
            cursor = conn.cursor()
            #match_data = json.load(f)
            for event in match_data['events']:
                home_player_id = event['homeTeam'].get('id')
                away_player_id = event['awayTeam'].get('id')
                round = None
                if event.get('roundInfo'):
                    round = event['roundInfo'].get('slug')
                tournament_id = event['tournament'].get('id')
                tournament = event['tournament']['uniqueTournament']
                tennisPoints = tournament.get('tennisPoints')
                tournamentName = tournament.get('name')
                category = tournament['category'].get('name')
                groundType = event['groundType']
                date = datetime.fromtimestamp(event.get('startTimestamp'))
                score = None
                winner_id = None
                if event['status'].get('type') == "finished":
                    print("finished")
                    score = f"{event['homeScore'].get('current')}:{event['awayScore'].get('current')} ("
                    for res in event['homeScore']:
                        if res.startswith("period") and len(res) == 7:
                            score += f"{event['homeScore'].get(res)}-{event['awayScore'].get(res)}, "
                    score = score[:-2] + ")"
                    if event.get('winnerCode') == 1:
                        winner_id = event['homeTeam'].get('id')
                    else:
                        winner_id = event['awayTeam'].get('id')
                try:
                    cursor.execute(
                        '''INSERT OR IGNORE INTO Matches (
                        match_id,
                        tournament_id, 
                        ground_type, 
                        match_date,
                        home_player_id,
                        away_player_id,
                        round,
                        winner_id,
                        score)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(match_id) DO UPDATE SET
                        winner_id = excluded.winner_id,
                        score = excluded.score,
                        match_date = excluded.match_date''',
                        (event.get('id'),
                         tournament_id,
                         groundType, date,
                         home_player_id,
                         away_player_id,
                         round,
                         winner_id,
                         score))
                    cursor.execute('''INSERT OR IGNORE INTO Tournaments (
                                            tournament_id, 
                                            name, category,
                                            tennisPoints, groundType) 
                                            VALUES (?, ?, ?, ?, ?)''',
                                   (tournament_id,
                                    tournamentName,
                                    category,
                                    tennisPoints,
                                    groundType))
                except sqlite3.Error as e:
                    print(f"An error occurred: {e}")
            conn.commit()

    def change_match_player_suffix(self, player_id, page):
        with sqlite3.connect(self.name) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''UPDATE Players SET history_fetched = ? WHERE player_id = ?''', (page, player_id))
                conn.commit()
            except sqlite3.Error as e:
                print(f"An error occurred: {e}")
            conn.commit()

    def add_match_stats(self, stats_data, match_id):
        # 1. Flatten the JSON data
        flat_stats = []
        for period_stats in stats_data['statistics']:
            period = period_stats['period']
            for group in period_stats['groups']:
                group_name = group['groupName']
                for stat_item in group['statisticsItems']:
                    flat_stats.append((
                        match_id,
                        period,
                        group_name,
                        stat_item.get('name', 'N/A'),
                        stat_item.get('home', 'N/A'),
                        stat_item.get('away', 'N/A')
                    ))

        # 2. Insert all rows into the database
        with sqlite3.connect(self.name) as conn:
            cursor = conn.cursor()
            try:
                # Use executemany to insert all stats at once
                sql = """
                INSERT OR IGNORE INTO MatchStats 
                (match_id, period, stat_group, stat_name, home_value, away_value)
                VALUES (?, ?, ?, ?, ?, ?)
                """
                cursor.executemany(sql, flat_stats)

                cursor.execute('''UPDATE Matches SET have_stats = ? WHERE match_id = ?''', (1, match_id))
                conn.commit()


                print(f"Successfully added {len(flat_stats)} stat rows for match {match_id}")
            except sqlite3.Error as e:
                print(f"An error occurred while adding stats: {e}")

    def add_additional_player_info(self, player_data):
        with (sqlite3.connect(self.name) as conn):
            cursor = conn.cursor()
            player = player_data['team']
            try:
                cursor.execute('''UPDATE Players
                                SET gender = ?, residence = ?, height = ?, weight = ?,
                                plays = ?, turnedPro = ?, prizeTotal = ?, birth = ?, last_update = ?
                                WHERE player_id = ?''',
                               (player['gender'],
                                player['playerTeamInfo'].get('residence'),
                                player['playerTeamInfo'].get('height'),
                                player['playerTeamInfo'].get('weight'),
                                player['playerTeamInfo'].get('plays'),
                                player['playerTeamInfo'].get('turnedPro'),
                                player['playerTeamInfo'].get('prizeTotal'),
                                datetime.fromtimestamp(player['playerTeamInfo'].get('birthDateTimestamp')),
                                datetime.today(),
                                player['id']))
            except sqlite3.Error as e:
                print(f"An error occurred: {e}")
            conn.commit()

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
            'ranking_id', 'ranking_date', 'rank', 'points', 'history_fetched'
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

    def find_players_missing_info(self, rank = 100, limit=1):
        """
        Finds players who are in the rankings but are missing
        their additional info in the Players table.

        Returns them sorted by rank.

        :param limit: How many player IDs to return.
        :param rank: Maximum rank of player
        :return: A list of tuples, e.g., [(player_id, name, rank), ...]
        """
        sql_query = """
        SELECT P.player_id
        FROM Rankings R
        JOIN Players P ON R.player_id = P.player_id
        WHERE P.Birth IS NULL AND R.rank <= ?
        ORDER BY R.ranking_date DESC, R.rank ASC
        LIMIT ?
        """

        with sqlite3.connect(self.name) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql_query, (rank, limit,))
                players = cursor.fetchall()
                return players
            except sqlite3.Error as e:
                print(f"An error occurred: {e}")
                return []


    def find_recent_unfinished_matches(self, lookback_days=3):
        """
        Finds all players who participated in an unfinished match
        that was scheduled within the last X days.

        Returns them sorted by their rank.

        :param lookback_days: How many days to look back (e.g., 3).
        :return: A list of tuples, e.g., [(player_id, rank, match_id), ...]
        """

        # This string will be '-3 days'
        lookback_str = f'-{lookback_days} days'

        sql_query = """
        SELECT P.player_id, R.rank, M.match_id
        FROM Matches M
        JOIN Players P ON P.player_id = M.home_player_id
        JOIN Rankings R ON R.player_id = P.player_id
        WHERE M.winner_id IS NULL                            -- Match is unfinished
          AND DATE(M.match_date) < DATE('now')               -- Match is in the past
          AND DATE(M.match_date) >= DATE('now', ?)           -- But only within the lookback window

        UNION

        SELECT P.player_id, R.rank, M.match_id
        FROM Matches M
        JOIN Players P ON P.player_id = M.away_player_id
        JOIN Rankings R ON R.player_id = P.player_id
        WHERE M.winner_id IS NULL 
          AND DATE(M.match_date) < DATE('now')
          AND DATE(M.match_date) >= DATE('now', ?)

        ORDER BY R.rank ASC;
        """

        with sqlite3.connect(self.name) as conn:
            cursor = conn.cursor()
            try:
                # We pass the lookback string as a parameter for both parts
                # of the UNION query
                cursor.execute(sql_query, (lookback_str, lookback_str))
                players = cursor.fetchall()
                return players
            except sqlite3.Error as e:
                print(f"An error occurred: {e}")
                return []

    def find_player_for_match_backfill(self, history=0):
        """
        Finds the single highest-ranked player whose match
        history has not been fetched yet.

        :return: A (player_id, name) tuple or None
        """
        sql_query = """
        SELECT P.player_id, P.name, P.history_fetched
        FROM Rankings R
        JOIN Players P ON R.player_id = P.player_id
        WHERE P.history_fetched = ? AND R.rank <= 100
        ORDER BY R.rank ASC
        LIMIT 1
        """

        with sqlite3.connect(self.name) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql_query, (history,))
                player = cursor.fetchone()
                return player  # e.g., (10400727, 'Carlos Alcaraz')
            except sqlite3.Error as e:
                print(f"An error occurred: {e}")
                return None

if __name__ == "__main__":
    database = Database("tennis.db")
    database.update_players_from_ranking()
    #database.fill_ranking("dd")
    #database.add_match("dd")
    #database.show_stats()
    #print(database.player_from_rank(1))
    #print(database.player_name_from_id(database.player_from_rank(1)))
    print(database.search('Players',
                         select_cols=['player_id'],
                         where_conditions={'name': 'Carlos Alcaraz'},
                         fetchone=True))
    print(database.search('Players'))
    #database.add_additional_player_info("dd")

    with open("rank/stats_10400727.json", "r", encoding='utf-8') as f:
        data = json.load(f)  # We're using the string above
        #database.add_match_stats(data, 14046430)