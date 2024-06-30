# Import all the necessary classes from the public submodule:
from .interface import TraitsInterface, TraitsUtilityInterface, TraitsKey, TrainStatus, SortingCriteria

# Import all the necessary default configurations
from .interface import BASE_USER_NAME, BASE_USER_PASS, ADMIN_USER_NAME, ADMIN_USER_PASS
from typing import List, Tuple, Optional
# Implement the utility class. Add any additional method that you need
class TraitsUtility(TraitsUtilityInterface):
    
    def __init__(self, rdbms_connection, rdbms_admin_connection, neo4j_driver) -> None:
        self.rdbms_connection = rdbms_connection
        self.rdbms_admin_connection = rdbms_admin_connection
        self.neo4j_driver = neo4j_driver

    @staticmethod
    def generate_sql_initialization_code() -> List[str]:
        # Note: this code ensures that users are recreated as needed. You need to add the proper permissions. Also add here the statements to setup the database.
        return [
            f"DROP USER IF EXISTS '{ADMIN_USER_NAME}'@'%';",
            f"DROP USER IF EXISTS '{BASE_USER_NAME}'@'%';",
            f"CREATE USER '{ADMIN_USER_NAME}'@'%' IDENTIFIED BY '{ADMIN_USER_PASS}';",
            f"CREATE USER '{BASE_USER_NAME}'@'%' IDENTIFIED BY '{BASE_USER_PASS}';",
            f"GRANT ALL PRIVILEGES ON *.* TO '{ADMIN_USER_NAME}'@'%' WITH GRANT OPTION;",
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON *.* TO '{BASE_USER_NAME}'@'%';",

            "CREATE TABLE IF NOT EXISTS users ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "email VARCHAR(255) UNIQUE NOT NULL,"
            "details VARCHAR(255)"
            ");",

            "CREATE TABLE IF NOT EXISTS trains ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "capacity INT NOT NULL,"
            "status INT NOT NULL"
            ");",

            "CREATE TABLE IF NOT EXISTS stations ("
            "id VARCHAR(50) PRIMARY KEY,"
            "details VARCHAR(255)"
            ");",

            "CREATE TABLE IF NOT EXISTS connections ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "start_station VARCHAR(50) NOT NULL,"
            "end_station VARCHAR(50) NOT NULL,"
            "travel_time INT NOT NULL,"
            "FOREIGN KEY (start_station) REFERENCES stations(id),"
            "FOREIGN KEY (end_station) REFERENCES stations(id)"
            ");",


            "CREATE TABLE IF NOT EXISTS tickets ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "user_id INT NOT NULL,"
            "connection_id INT NOT NULL,"
            "reserved_seats BOOLEAN NOT NULL,"
            "FOREIGN KEY (user_id) REFERENCES users(id)"
            ");",

            "CREATE TABLE IF NOT EXISTS purchase_history ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "user_email VARCHAR(255) NOT NULL,"
            "travel_date DATETIME," 
            "details VARCHAR(255)"
            ");"
            # Add SQL commands for creating tables, etc.
        ]

    def get_all_users(self) -> List:
        with self.rdbms_connection.cursor() as cursor:
            cursor.execute("SELECT * FROM users;")
            return cursor.fetchall()

    def get_all_schedules(self) -> List:
        with self.neo4j_driver.session() as session:
            result = session.run("MATCH (s:Schedule) RETURN s")
            schedules = [record["s"] for record in result]
            return schedules

    
class Traits(TraitsInterface):

    def __init__(self, rdbms_connection, rdbms_admin_connection, neo4j_driver) -> None:
        self.connection = rdbms_connection
        self.rdbms_admin_connection = rdbms_admin_connection
        self.neo4j_driver = neo4j_driver

    
    def search_connections(self, starting_station_key: TraitsKey, ending_station_key: TraitsKey,
                           travel_time_day: int = None, travel_time_month: int = None,
                           travel_time_year: int = None, travel_time_hour: int = None,
                           travel_time_minute: int = None, is_departure_time: bool = True,
                           sort_by: SortingCriteria = SortingCriteria.OVERALL_TRAVEL_TIME,
                           is_ascending: bool = True, limit: int = 5) -> List:
        with self.connection.cursor() as cursor:
            station_query = """
            SELECT COUNT(*) FROM stations WHERE id IN (%s, %s);
            """
            cursor.execute(station_query, (starting_station_key.to_string(), ending_station_key.to_string()))
            station_count = cursor.fetchone()[0]
            
            if station_count < 2:
                raise ValueError("One or both stations do not exist.")
            
            query = """
            SELECT * FROM connections 
            WHERE start_station = %s AND end_station = %s
            ORDER BY travel_time
            LIMIT %s;
            """
            cursor.execute(query, (starting_station_key.to_string(), ending_station_key.to_string(), limit))
            connections = cursor.fetchall()
            
            return connections

    def get_train_current_status(self, train_key: TraitsKey) -> Optional[TrainStatus]:
        with self.connection.cursor() as cursor:
            query = "SELECT status FROM trains WHERE id = %s;"
            cursor.execute(query, (train_key.to_string(),))
            result = cursor.fetchone()
            return TrainStatus(result[0]) if result else None

    def buy_ticket(self, user_email: str, connection, also_reserve_seats: bool = True):
        with self.connection.cursor() as cursor:
            query = "SELECT id FROM users WHERE email = %s;"
            cursor.execute(query, (user_email,))
            user = cursor.fetchone()
            if not user:
                raise ValueError("User does not exist")

            ticket_query = """
            INSERT INTO tickets (user_id, connection_id, reserved_seats)
            VALUES (%s, %s, %s);
            """
            cursor.execute(ticket_query, (user['id'], connection['id'], also_reserve_seats))
            self.connection.commit()

    def get_purchase_history(self, user_email: str) -> List:
        with self.connection.cursor() as cursor:
            query = """
            SELECT * FROM purchase_history 
            WHERE user_email = %s
            ORDER BY travel_date DESC;
            """
            cursor.execute(query, (user_email,))
            return cursor.fetchall()

    def add_user(self, user_email: str, user_details) -> None:
        if '@' not in user_email:
            raise ValueError("Invalid email format")

        with self.connection.cursor() as cursor:
            query = "SELECT id FROM users WHERE email = %s;"
            cursor.execute(query, (user_email,))
            if cursor.fetchone():
                raise ValueError("User already exists")

            insert_query = "INSERT INTO users (email, details) VALUES (%s, %s);"
            cursor.execute(insert_query, (user_email, user_details))
            self.connection.commit()

    def delete_user(self, user_email: str) -> None:
        with self.connection.cursor() as cursor:
            delete_query = "DELETE FROM users WHERE email = %s;"
            cursor.execute(delete_query, (user_email,))
            self.connection.commit()

    def add_train(self, train_key: TraitsKey, train_capacity: int, train_status: TrainStatus) -> TraitsKey:
        with self.connection.cursor() as cursor:
            if train_key is not None:
                query = "SELECT id FROM trains WHERE id = %s;"
                cursor.execute(query, (train_key.to_string(),))
                if cursor.fetchone():
                    raise ValueError("Train already exists")

                insert_query = """
                INSERT INTO trains (id, capacity, status) 
                VALUES (%s, %s, %s);
                """
                cursor.execute(insert_query, (train_key.to_string(), train_capacity, train_status.value))
            else:
                insert_query = """
                INSERT INTO trains (capacity, status) 
                VALUES (%s, %s);
                """
                cursor.execute(insert_query, (train_capacity, train_status.value))

            self.connection.commit()

            if train_key is None:
                train_key = TraitsKey(cursor.lastrowid)

            return train_key

    def update_train_details(self, train_key: TraitsKey, train_capacity: Optional[int] = None,
                             train_status: Optional[TrainStatus] = None) -> None:
        with self.connection.cursor() as cursor:
            updates = []
            params = []

            if train_capacity is not None:
                updates.append("capacity = %s")
                params.append(train_capacity)
            if train_status is not None:
                updates.append("status = %s")
                params.append(train_status.value)

            if updates:
                query = f"UPDATE trains SET {', '.join(updates)} WHERE id = %s;"
                params.append(train_key.to_string())
                cursor.execute(query, params)
                self.connection.commit()

    def delete_train(self, train_key: TraitsKey) -> None:
        with self.connection.cursor() as cursor:
            delete_query = "DELETE FROM trains WHERE id = %s;"
            cursor.execute(delete_query, (train_key.to_string(),))
            self.connection.commit()

    def add_train_station(self, train_station_key: TraitsKey, train_station_details) -> TraitsKey:
        with self.connection.cursor() as cursor:
            query = "SELECT id FROM stations WHERE id = %s;"
            cursor.execute(query, (train_station_key.to_string(),))
            if cursor.fetchone():
                raise ValueError("Station already exists")

            insert_query = "INSERT INTO stations (id, details) VALUES (%s, %s);"
            cursor.execute(insert_query, (train_station_key.to_string(), train_station_details))
            self.connection.commit()
            return train_station_key

    def connect_train_stations(self, starting_train_station_key: TraitsKey, ending_train_station_key: TraitsKey, travel_time_in_minutes: int) -> None:
        with self.connection.cursor() as cursor:
            station_query = """
            SELECT COUNT(*) FROM stations WHERE id IN (%s, %s);
            """
            cursor.execute(station_query, (starting_train_station_key.to_string(), ending_train_station_key.to_string()))
            station_count = cursor.fetchone()[0]
            
            if station_count < 2:
                raise ValueError("One or both stations do not exist.")

            try:
                insert_query = """
                INSERT INTO connections (start_station, end_station, travel_time)
                VALUES (%s, %s, %s);
                """
                cursor.execute(insert_query, (starting_train_station_key.to_string(), ending_train_station_key.to_string(), travel_time_in_minutes))
                self.connection.commit()
            except mysql.connector.Error as err:
                print(f"Error: {err}")
                self.connection.rollback()
                raise

    def add_schedule(self, train_key: TraitsKey, starting_hours_24_h: int, starting_minutes: int,
                     stops: List[Tuple[TraitsKey, int]], valid_from_day: int, valid_from_month: int,
                     valid_from_year: int, valid_until_day: int, valid_until_month: int,
                     valid_until_year: int) -> None:
        with self.neo4j_driver.session() as session:
            if len(stops) < 2:
                raise ValueError("Schedule must have at least two stops")

            if train_key is None:
                raise ValueError("Invalid train_key")

            for i in range(len(stops) - 1):
                start_station = stops[i][0].to_string()
                end_station = stops[i + 1][0].to_string()
                connections = self.search_connections(TraitsKey(start_station), TraitsKey(end_station))

                if not connections:
                    raise ValueError(f"Stops {start_station} and {end_station} are not connected")

            session.run(
                """
                CREATE (s:Schedule {trainKey: $trainKey, startTime: $startTime, validFrom: $validFrom, validUntil: $validUntil})
                """,
                trainKey=train_key.to_string(),
                startTime=f"{starting_hours_24_h:02}:{starting_minutes:02}",
                validFrom=f"{valid_from_year}-{valid_from_month:02}-{valid_from_day:02}",
                validUntil=f"{valid_until_year}-{valid_until_month:02}-{valid_until_day:02}",
            )

    def get_train(self, train_key: TraitsKey) -> Optional[dict]:
        with self.connection.cursor(dictionary=True) as cursor:
            query = "SELECT * FROM trains WHERE id = %s;"
            cursor.execute(query, (train_key,))
            train = cursor.fetchone()
            
            if not train:
                return None

        schedules_collection = self.neo4j_driver['schedules']
        schedules = list(schedules_collection.find({'train_key': train_key.to_string()}))
        
        train['schedules'] = schedules
        return train