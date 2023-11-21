#
# CS 460: Problem Set 4: XQuery Programming Problems
#

#
# For each query, use a text editor to add the appropriate XQuery
# command between the triple quotes provided for that query's variable.
#
# For example, here is how you would include a query that finds
# the names of all movies in the database from 1990.
#
sample = """
    for $m in //movie
    where $m/year = 1990
    return $m/name
"""

#
# 1. Put your query for this problem between the triple quotes found below.
#    Follow the same format as the model query shown above.
#
query1 = """
    //person[contains(pob, "Canada")]/@directed/../name
"""

#
# 2. Put your query for this problem between the triple quotes found below.
#
query2 = """
    let $distinct-directors :=
    for $m in //movie,
        $director_id in $m/@directors
    for $person in //person[@id=$director_id]
    where contains($person/pob, "Canada")
    return concat($person/name, ' (', $person/pob, ')')

    let $unique-directors := distinct-values($distinct-directors)

    for $director in $unique-directors
    order by $director
    return <canadian_dir>{$director}</canadian_dir>
"""

#
# 3. Put your query for this problem between the triple quotes found below.
#
query3 = """
    for $m in //movie
    for $director_id in tokenize($m/@directors, '\s+')
    for $person in //person[@id=$director_id]
    where contains($person/pob, "Canada")
    let $director_name := $person/name
    group by $director_name
    let $pob := $person/pob
    let $directed_movies := (
        for $dir_id in ($m/@directors)
        for $dir_movie in //movie[@directors=$dir_id]
        order by $dir_movie/name
        return ($dir_movie)
    )
    let $avg_runtime := avg($directed_movies/runtime)
    order by $director_name
    return
        <canadian_dir>
        <name>{$director_name}</name>
        {$pob}
        {
            for $movie in distinct-values($directed_movies/name)
            return <directed>{$movie}</directed>
        }
        <avg_runtime>{$avg_runtime}</avg_runtime>
        <num_top_grossers>{count($directed_movies/earnings_rank)}</num_top_grossers>
        </canadian_dir>
"""

#
# 4. Put your query for this problem between the triple quotes found below.
#
query4 = """
    for $p in //person
    for $o1 in //oscar
    for $o2 in //oscar
    where $p/@id = $o1/@person_id and $p/@id = $o2/@person_id and $o1/year + 1 = $o2/year
    return
    <back_to_back>
        <name>{data($p/name)}</name>
        <first_win>{data($o1/type)} ({data($o1/year)})</first_win>
        <second_win>{data($o2/type)} ({data($o2/year)})</second_win>
    </back_to_back>
"""
