#
# CS 460: Problem Set 5: MongoDB Query Problems
#

#
# For each query, use a text editor to add the appropriate XQuery
# command between the triple quotes provided for that query's variable.
#
# For example, here is how you would include a query that finds
# the names of all movies in the database from 1990.
#
sample = """
    db.movies.find( { year: 1990 }, 
                    { name: 1, _id: 0 } )
"""

#
# 1. Put your query for this problem between the triple quotes found below.
#    Follow the same format as the model query shown above.
#
query1 = """
    db.people.find({pob: /Florida, USA/},
               {name: 1,
                pob: 1,
                _id: 0})
"""

#
# 2. Put your query for this problem between the triple quotes found below.
#
query2 = """
    db.movies.find({ "year": 2023, "earnings_rank": { $exists: true }}, 
                   {"name": 1, "earnings_rank": 1, "_id": 0}).sort({"earnings_rank": 1})

"""

#
# 3. Put your query for this problem between the triple quotes found below.
#
query3 = """
    db.movies.find({"actors.name": "Julianne Moore"}, 
    {"name": 1, "year": 1, "rating": 1, "_id": 0})

"""

#
# 4. Put your query for this problem between the triple quotes found below.
#
query4 = """
    db.oscars.find({"type": "BEST-PICTURE", "year": { $gte: 2020 }}, 
                   {"year": 1, "movie.name": 1, "_id": 0})

"""

#
# 5. Put your query for this problem between the triple quotes found below.
#
query5 = """
    db.people.count({"hasDirected": true,
                    $or: [{ "dob": { $regex: /^....-11-../ } },
                          { "dob": { $regex: /^....-12-../ } }]
                   })

"""

#
# 6. Put your query for this problem between the triple quotes found below.
#
query6 = """
    db.movies.aggregate([
        {
            $group: {
            _id: null,
            shortestMovie: { $min: { runtime: "$runtime", name: "$name" } }
            }
        },
        {
            $project: {
            _id: 0,
            name: "$shortestMovie.name",
            runtime: "$shortestMovie.runtime"
            }
        }
    ])
    
"""

#
# 7. Put your query for this problem between the triple quotes found below.
#
query7 = """
    db.oscars.aggregate([
    {
        $group: {
        _id: "$movie.id",
		movie: { $first: "$movie.name"},
        num_awards: { $sum: 1 },
        types: { $push: "$type" }
        }
    },
    {
        $match: {
        num_awards: { $gte: 4 }
        }
    },
    {
        $project: {
        _id: 0,
        num_awards: 1,
        types: 1,
        movie: "$movie"
        }
    }
    ])
"""

#
# 8. Put your query for this problem between the triple quotes found below.
#
query8 = """
    db.movies.aggregate([
    {
        $match: {
        year: { $gte: 2010, $lte: 2020 }
        }
    },
    {
        $group: {
        _id: "$year",
        num_movies: { $sum: 1 },
        total_runtime: { $sum: "$runtime" },
        max_earnings_rank: { $min: "$earnings_rank" }
        }
    },
    {
        $project: {
        _id: 0,
        year: "$_id",
        num_movies: 1,
        avg_runtime: { $divide: ["$total_runtime", "$num_movies"] },
        best_rank: "$max_earnings_rank"
        }
    },
    {
        $sort: {
        year: 1
        }
    }
    ])
"""

#
# 9. Put your query for this problem between the triple quotes found below.
#
query9 = """
    db.movies.aggregate([
        {
            $match: {
                "genre": /N/
            }
        },
        {
            $unwind: "$actors"
        },
        {
            $group: {
                _id: "$actors.name",
                movies: { $addToSet: "$movie.name" },
                num_animated: { $sum: 1 }
            }
        },
        {
            $match: {
                num_animated: { $gte: 3 }
            }
        },
        {
            $project: {
                _id: 0,
                actor: "$_id",
                num_animated: 1
            }
        },
        {
            $sort: {
                num_animated: -1,
                            actor: 1
            }
        }
    ])
"""

#
# 10. Put your query for this problem between the triple quotes found below.
#
query10 = """
    db.people.aggregate([
        {
            $match: {
                pob: { $exists: true, $ne: null }
            }
        },
        {
            $project: {
                country: { $arrayElemAt: [{ $split: ["$pob", ", "] }, -1] }
            }
        },
        {
            $group: {
                _id: "$country",
                num_born: { $sum: 1 }
            }
        },
        {
            $sort: {
                num_born: -1
            }
        },
        {
            $limit: 5
        },
        {
            $project: {
                _id: 0,
                num_born: 1,
                country: "$_id"
            }
        }
    ])
"""
