""" Includes functions for your DB queries (query NUM). """


def query_1(connection, keyword, limit=10):
    """
    Searches for movies by overview keyword and ranks results by relevance and popularity.
    """
    if not isinstance(limit, int) or limit <= 0:  # Validate limit input
        limit = 10
    query = """
            SELECT movie_id, title, overview, popularity, relevance
            FROM (
                SELECT movie_id, title, overview, popularity, 
                       MATCH(overview) AGAINST (%s IN NATURAL LANGUAGE MODE) AS relevance
                FROM Movies
            ) AS subquery
            WHERE relevance > 0
            ORDER BY relevance DESC, popularity DESC    -- break ties by movie popularity
            LIMIT %s;
            """
    cursor = connection.cursor()
    try:
        cursor.execute(query, (keyword, limit))     # execute using prepared statement
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
    except Exception as e:
        print(f"Error executing query_1: {e}")
        return [], []
    finally:
        cursor.close()
    return results, column_names


def query_2(connection, keyword):
    """
    Finds actors with the same name (e.g. last or first) and counts how many movies they appeared in.
    Use Case: identifying acting dynasties (e.g., Fonda, Skarsgård)
    """
    query = """
            SELECT a.actor_id, a.name, COUNT(ma.movie_id) AS movie_count
            FROM Actors a
            JOIN Movies_Actors ma ON a.actor_id = ma.actor_id
            WHERE MATCH(a.name) AGAINST (%s IN NATURAL LANGUAGE MODE)
            GROUP BY a.actor_id, a.name
            ORDER BY movie_count DESC;
            """
    cursor = connection.cursor()
    try:
        cursor.execute(query, (keyword,))
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
    except Exception as e:
        print(f"Error executing query_2: {e}")
        return [], []
    finally:
        cursor.close()
    return results, column_names


def query_3(connection, genre):
    """
    Finds the top 5 most profitable movies in a genre and compares them to the genre's average profit.
    """
    query = """
            SELECT 
                m.movie_id, 
                m.title, 
                (m.revenue - m.budget) AS profit,
                AVG(m.revenue - m.budget) OVER (PARTITION BY g.genre_name) AS avg_profit
            FROM Movies m
            JOIN Movies_Genres mg ON m.movie_id = mg.movie_id
            JOIN Genres g ON mg.genre_id = g.genre_id
            WHERE g.genre_name = %s
            ORDER BY profit DESC
            LIMIT 5;
            """
    cursor = connection.cursor()
    try:
        cursor.execute(query, (genre,))
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
    except Exception as e:
        print(f"Error executing query_3: {e}")
        return [], []
    finally:
        cursor.close()
    return results, column_names


def query_4(connection, genre):
    """
    Finds 10 actors who appeared in movies of a given genre with a vote average above the genre's average.
    The actors are ordered by the number of genre's high-rated movies they have appeared in.
    """

    # the CTE ensures avg_vote is computed once and will be available for all rows
    # only considers movies from the given genre for counting
    # only counts movies rated higher than the genre's average
    query = """
            WITH GenreAvg AS (
                SELECT AVG(m.vote_average) AS avg_vote
                FROM Movies m
                JOIN Movies_Genres mg ON m.movie_id = mg.movie_id
                JOIN Genres g ON mg.genre_id = g.genre_id
                WHERE g.genre_name = %s
            )
            SELECT a.actor_id, a.name, COUNT(m.movie_id) AS high_rated_movies
            FROM Actors a
            JOIN Movies_Actors ma ON a.actor_id = ma.actor_id
            JOIN Movies m ON ma.movie_id = m.movie_id
            JOIN Movies_Genres mg ON m.movie_id = mg.movie_id
            JOIN Genres g ON mg.genre_id = g.genre_id
            JOIN GenreAvg ON 1=1
            WHERE g.genre_name = %s
            AND m.vote_average > GenreAvg.avg_vote
            GROUP BY a.actor_id, a.name
            ORDER BY high_rated_movies DESC
            LIMIT 10;
            """
    cursor = connection.cursor()
    try:
        cursor.execute(query, (genre, genre))
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
    except Exception as e:
        print(f"Error executing query_4: {e}")
        return [], []
    finally:
        cursor.close()
    return results, column_names


def query_5(connection):
    """
    Finds the top 5 production companies by movie count and their total revenue.
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
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
    except Exception as e:
        print(f"Error executing query_5: {e}")
        return [], []
    finally:
        cursor.close()
    return results, column_names


def query_6(connection, movie_title, limit=10):
    """
    Suggests movies related to the given title based on shared keywords, ranked by popularity.
    If several movies with given title exist, chooses the most popular.
    """
    if not isinstance(limit, int) or limit <= 0:  # Validate limit input
        limit = 10
    query = """
            WITH MovieID AS (
                SELECT movie_id
                FROM Movies
                WHERE title = %s
                ORDER BY popularity DESC
                LIMIT 1
            ),
            MovieKeywords AS (
                SELECT mk.keyword_id
                FROM Movies_Keywords mk
                JOIN MovieID mid ON mk.movie_id = mid.movie_id
            )
            SELECT m.movie_id, m.title, m.popularity, COUNT(mk.keyword_id) AS shared_keywords
            FROM Movies_Keywords mk
            JOIN MovieKeywords mk_ref ON mk.keyword_id = mk_ref.keyword_id
            JOIN Movies m ON mk.movie_id = m.movie_id
            WHERE mk.movie_id NOT IN (SELECT movie_id FROM MovieID)
            GROUP BY m.movie_id, m.title, m.popularity
            ORDER BY shared_keywords DESC, m.popularity DESC
            LIMIT %s;
            """
    cursor = connection.cursor()
    try:
        cursor.execute(query, (movie_title, limit))
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        if not results:
            print(f"No recommendations found for '{movie_title}'.")
    except Exception as e:
        print(f"Error executing query_6: {e}")
        return [], []
    finally:
        cursor.close()
    return results, column_names

