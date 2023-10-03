#
# CS 460: Problem Set 1, SQL Programming Problems
#

#
# For each problem, use a text editor to add the appropriate SQL
# command between the triple quotes provided for that problem's variable.
#
# For example, here is how you would include a query that finds the
# names and years of all movies in the database with an R rating:
#
sample = """
    SELECT name, year
    FROM Movie
    WHERE rating = 'R';
"""

#
# Problem 4. Put your SQL command between the triple quotes found below.
#
problem4 = """
    SELECT P.name, P.pob, P.dob
    FROM Person AS P
    WHERE P.name IN ('Michelle Yeoh', 'Jamie Lee Curtis');
"""
"""
    Output:
    Jamie Lee Curtis	Los Angeles, California, USA	1958-11-22
    Michelle Yeoh	Ipoh, Perak, Malaysia	1962-08-06
        
"""
#
# Problem 5. Put your SQL command between the triple quotes found below.
#
problem5 = """
    SELECT M.name, O.year
    FROM Movie AS M, Oscar AS O
    WHERE O.movie_id = M.id
    AND O.type LIKE 'BEST-PICTURE' 
    AND O.year BETWEEN 2010 AND 2019
    ORDER BY O.year;
"""
'''
    Output:
    Green Book	2019
    The Shape of Water	2018
    Moonlight	2017
    Spotlight	2016
    Birdman: Or	2015
    12 Years a Slave	2014
    Argo	2013
    The Artist	2012
    The King's Speech	2011
    The Hurt Locker	2010
'''


#
# Problem 6. Put your SQL command between the triple quotes found below.
#
problem6 = """
    SELECT year, name FROM Movie
    WHERE id IN 
            (SELECT movie_id FROM Oscar WHERE person_id IN
                (SELECT id FROM Person WHERE name LIKE 'Steven Spielberg') 
                    AND type LIKE 'BEST_DIRECTOR')
"""

"""
    Output:
    1993	Schindler's List
    1998	Saving Private Ryan
"""

#
# Problem 7. Put your SQL command between the triple quotes found below.
#
problem7 = """
    SELECT COUNT(DISTINCT M.id)
    FROM Movie M
    JOIN Actor A ON M.id = A.movie_id
    JOIN Person P ON A.actor_id = P.id
    WHERE P.pob IS NOT NULL AND P.pob NOT LIKE '%USA';
"""
""" 
    Output:
    541
"""
#
# Problem 8. Put your SQL command between the triple quotes found below.
#
problem8 = """
    SELECT M.name, M.runtime
    FROM Movie M
    WHERE M.genre LIKE '%N%'
    AND M.runtime = (
        SELECT MAX(runtime)
        FROM Movie
        WHERE genre LIKE '%N%'
    );
"""
"""
    Output:
    Spider-Man: Across the Spider-Verse	140    
"""
#
# Problem 9. Put your SQL command between the triple quotes found below.
#
problem9 = """
    SELECT M.year, M.name AS movie_name, COUNT(*) AS oscars_won
    FROM Movie M
    JOIN Oscar O ON M.id = O.movie_id
    WHERE (M.year, M.name) IN (
        SELECT M2.year, M2.name
        FROM Movie M2
        JOIN Oscar O2 ON M2.id = O2.movie_id
        WHERE O2.type IN ('BEST-PICTURE', 'BEST-DIRECTOR', 'BEST-ACTRESS', 'BEST-SUPPORTING-ACTRESS', 'BEST-ACTOR', 'BEST-SUPPORTING-ACTOR')
        GROUP BY M2.year, M2.name
        HAVING COUNT(DISTINCT O2.type) >= 5
    )
    GROUP BY M.year, M.name
    HAVING COUNT(*) >= 5;           
"""
"""
    Output:
    2022	Everything Everywhere All at Once	6
"""
#
# Problem 10. Put your SQL command between the triple quotes found below.
#
problem10 = """
    SELECT P.name AS director_name, P.pob AS place_of_birth
    FROM Person P
    JOIN Director D ON P.id = D.director_id
    WHERE P.pob LIKE '%, France';
"""
"""
    Output:
    Frank Darabont	Montebeliard, France
    Roman Polanski	Paris, France
    Jonathan Kaplan	Paris, France
    William Wyler	Mulhouse, Haut-Rhin, Alsace, France
    Roman Polanski	Paris, France
    William Wyler	Mulhouse, Haut-Rhin, Alsace, France
    William Wyler	Mulhouse, Haut-Rhin, Alsace, France
    William Wyler	Mulhouse, Haut-Rhin, Alsace, France
    William Wyler	Mulhouse, Haut-Rhin, Alsace, France
    William Wyler	Mulhouse, Haut-Rhin, Alsace, France
    William Wyler	Mulhouse, Haut-Rhin, Alsace, France
    William Wyler	Mulhouse, Haut-Rhin, Alsace, France
    William Wyler	Mulhouse, Haut-Rhin, Alsace, France
    Olivier Dahan	La Ciotat, Bouches-du-Rhone, France
    Louis Leterrier	Paris, France
    Michel Hazanavicius	Paris, France
    Florian Zeller	Paris, France
"""
#
# Problem 11. Put your SQL command between the triple quotes found below.
#
problem11 = """
    WITH Top25GrossingMovies AS (
        SELECT id, name, earnings_rank
        FROM Movie
        ORDER BY earnings_rank
        LIMIT 25
    )

    SELECT TGM.earnings_rank, TGM.name AS movie_name, O.type AS award_type
    FROM Top25GrossingMovies TGM
    LEFT JOIN Oscar O ON TGM.id = O.movie_id
    ORDER BY TGM.earnings_rank, award_type; 
"""
"""
    Output:
	NULL    Mission: Impossible II  NULL
	NULL    A Star Is Born  NULL
	NULL    X2: X-Men United    NULL
	NULL    Dr. Seuss' The Lorax    NULL	
        ...
    NULL	The Amazing Spider-Man 2	NULL
	NULL    World War Z     NULL
        
"""
#
# Problem 12. Put your SQL command between the triple quotes found below.
#
problem12 = """
    WITH BestPictureWinners AS (
        SELECT M.id
        FROM Movie M
        JOIN Oscar O ON M.id = O.movie_id
        WHERE O.type = 'BEST-PICTURE'
    )

    SELECT COUNT(*) AS num_longer_runtimes
    FROM Movie M
    WHERE M.id IN (SELECT id FROM BestPictureWinners)
    AND M.runtime > (
        SELECT AVG(runtime)
        FROM Movie
    );
"""
"""
    Output:
    56
"""
#
# Problem 13. Put your SQL command between the triple quotes found below.
#
problem13 = """
    SELECT
        'BEST-PICTURE' AS award_type,
        NULL AS person_name,
        M.name AS movie_name
    FROM
        Movie M
    JOIN
        Oscar O ON M.id = O.movie_id
    WHERE
        O.type = 'BEST-PICTURE'
        AND M.year = 1993

    UNION ALL

    SELECT
        O.type AS award_type,
        CASE
            WHEN O.type = 'BEST-PICTURE' THEN NULL
            ELSE P.name
        END AS person_name,
        M.name AS movie_name
    FROM
        Oscar O
    JOIN
        Person P ON O.person_id = P.id
    JOIN
        Movie M ON O.movie_id = M.id
    WHERE
        (O.type = 'BEST-DIRECTOR'
        OR O.type = 'BEST-ACTOR'
        OR O.type = 'BEST-ACTRESS'
        OR O.type = 'BEST-SUPPORTING-ACTOR'
        OR O.type = 'BEST-SUPPORTING-ACTRESS'
        OR O.type = 'BEST-PICTURE')
        AND M.year = 1993;            
"""
"""
    Output:
    BEST-PICTURE	NULL	Schindler's List
    BEST-ACTOR	Tom Hanks	Philadelphia
    BEST-ACTRESS	Holly Hunter	Piano, The
    BEST-SUPPORTING-ACTOR	Tommy Lee Jones	Fugitive, The
    BEST-SUPPORTING-ACTRESS	Anna Paquin	Piano, The
    BEST-DIRECTOR	Steven Spielberg	Schindler's List

"""
#
# Problem 14. Put your SQL command between the triple quotes found below.
#
problem14 = """
    SELECT COUNT(*) AS num_supporting_winners
    FROM (
        SELECT P.id
        FROM Person P
        JOIN Oscar O ON P.id = O.person_id
        WHERE (O.type = 'BEST-SUPPORTING-ACTOR' OR O.type = 'BEST-SUPPORTING-ACTRESS')
        AND P.id NOT IN (
            SELECT O2.person_id
            FROM Oscar O2
            WHERE O2.type IN ('BEST-ACTOR', 'BEST-ACTRESS')
        )
        GROUP BY P.id
        HAVING COUNT(DISTINCT O.type) = 1
    ) AS SupportingOnlyWinners;

"""
"""
    Output:
    150
"""
#
# Problem 15. Put your SQL command between the triple quotes found below.
#
problem15 = """
    INSERT INTO Actor (actor_id, movie_id)
    VALUES (0614165, 0468569)
"""
