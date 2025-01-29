""" Includes functions for your DB queries (query NUM ). """


def query_1(connection, keyword):
    """
    searches for movies by overview keyword and also ranks results by relevance and popularity
    """
    query = """
            SELECT movie_id, title, overview, popularity, 
                   MATCH(overview) AGAINST (%s IN NATURAL LANGUAGE MODE) AS relevance
            FROM Movies 
            WHERE MATCH(overview) AGAINST (%s IN NATURAL LANGUAGE MODE)
            ORDER BY relevance DESC, popularity DESC
            LIMIT 10;
        """
    cursor = connection.cursor()
    cursor.execute(query, (keyword, keyword))
    return cursor.fetchall()

def query_2(conn, keyword):
    """
    finds actors by name and counts how many movies they appeared in.
    """
    query = """
            SELECT a.actor_id, a.name, COUNT(ma.movie_id) AS movie_count
            FROM Actors a
            JOIN Movies_Actors ma ON a.actor_id = ma.actor_id
            WHERE MATCH(a.name) AGAINST (%s IN NATURAL LANGUAGE MODE)
            GROUP BY a.actor_id, a.name
            ORDER BY movie_count DESC;
            """
    cursor = conn.cursor()
    cursor.execute(query, (keyword,))
    return cursor.fetchall()

def query_3(conn, genre):
    """
    finds the top 5 most profitable movies in a genre and compares them to the genre's
    """
    query = """
            SELECT m.movie_id, m.title, (m.revenue - m.budget) AS profit,
                   (SELECT AVG(m2.revenue - m2.budget) 
                    FROM Movies m2
                    JOIN Movies_Genres mg2 ON m2.movie_id = mg2.movie_id
                    JOIN Genres g2 ON mg2.genre_id = g2.genre_id
                    WHERE g2.genre_name = %s) AS avg_profit
            FROM Movies m
            JOIN Movies_Genres mg ON m.movie_id = mg.movie_id
            JOIN Genres g ON mg.genre_id = g.genre_id
            WHERE g.genre_name = %s
            ORDER BY profit DESC
            LIMIT 5;
            """
    cursor = conn.cursor()
    cursor.execute(query, (genre, genre))
    return cursor.fetchall()

def query_4(conn, genre):
    """
    finds actors who appeared in movies with a vote average above the genre's average.
    """
    query = """
            SELECT DISTINCT a.actor_id, a.name, COUNT(m.movie_id) AS high_rated_movies
            FROM Actors a
            JOIN Movies_Actors ma ON a.actor_id = ma.actor_id
            JOIN Movies m ON ma.movie_id = m.movie_id
            WHERE m.vote_average > 
                  (SELECT AVG(m2.vote_average) 
                   FROM Movies m2
                   JOIN Movies_Genres mg ON m2.movie_id = mg.movie_id
                   JOIN Genres g ON mg.genre_id = g.genre_id
                   WHERE g.genre_name = %s)
            GROUP BY a.actor_id, a.name
            ORDER BY high_rated_movies DESC
            LIMIT 10;
            """
    cursor = conn.cursor()
    cursor.execute(query, (genre,))
    return cursor.fetchall()

def query_5(conn):
    """
    finds the top production companies by movie count and their total revenue.
    """
    query = """
            SELECT pc.production_company_name, 
                   COUNT(mpc.movie_id) AS movie_count, 
                   SUM(m.revenue) AS total_revenue,
                   (SUM(m.revenue) / COUNT(mpc.movie_id)) AS avg_revenue_per_movie
            FROM Production_Companies pc
            JOIN Movies_Production_Companies mpc ON pc.production_company_id = mpc.production_company_id
            JOIN Movies m ON m.movie_id = mpc.movie_id
            GROUP BY pc.production_company_id, pc.production_company_name
            HAVING COUNT(mpc.movie_id) > 5
            ORDER BY total_revenue DESC, avg_revenue_per_movie DESC
            LIMIT 5;
            """
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

