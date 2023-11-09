#
# CS 460: Problem Set 3: XQuery Programming Problems
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
    //person[contains(dob, '-11-26')]/name
"""

#
# 2. Put your query for this problem between the triple quotes found below.
#
query2 = """
    for $o in //oscar,$m in //movie
    where $m/@id = $o/@movie_id and $o/year >= 2010 and $o/year <= 2019 and $o/type="BEST-PICTURE"
    order by $o/year
    return <best_picture>{$o/year,$m/name}</best_picture>
"""

#
# 3. Put your query for this problem between the triple quotes found below.
#
query3 = """
    for $o in //oscar, $m in //movie
    where $m/@id = $o/@movie_id and $o/year = 1993
    return <oscar_93> 
    {
    $o/type,
    <movie>{$m/name/text()}</movie>,
        
        for $p in //person, $o2 in //oscar
        where $o2/year = 1993 and $o2/type = $o/type and $o2/@person_id = $p/@id
        
        return <person>{$p/name/text()}</person>
    }
    </oscar_93>
"""

#
# 4. Put your query for this problem between the triple quotes found below.
#
query4 = """
    for $y in distinct-values(//movie/year)
    where $y > 2009
    let $mfy := //movie[year=$y],$oy:=//oscar[year=$y+1]
    order by $y
    return <year_summary>{<year>{$y}</year>,
    <num_movies>{count($mfy/@id)}</num_movies>,
    <avg_runtime>{avg($mfy/runtime)}</avg_runtime>,
    let $DistinctOscarwinners  :=
    for $o in $oy
    let $m := //movie[@id = $o/@movie_id]
    return $m/name/text()
    return
        for $w in distinct-values($DistinctOscarwinners)
        return <oscar_winner>{$w}</oscar_winner>
    }</year_summary>
"""

#
# 5. Put your query for this problem between the triple quotes found below.
#
query5 = """
    for $m in //movie
    order by $m/year + 1 
    where count(//oscar[@movie_id = $m/@id]) > 3
    return <many_oscars>{
    <movie>{$m/name/text()}</movie>,
    <year>{$m/year + 1}</year>,
    for $o in //oscar
    where $o/year = $m/year+1 and $o/@movie_id = $m/@id
    return $o/type
    }</many_oscars>
"""
