const mongo = require('mongodb');
const MongoClient = mongo.MongoClient;
const url = 'mongodb://localhost:27017';

async function execute(){
  const client = await MongoClient.connect(url, { useNewUrlParser: true })
    .catch(err => { console.log(err); });
  if (!client) {
    return;
  }

  try {
    const db = client.db("test");
    let query = await q1(db);
    console.log("q1:");
    console.log(query);
    
    query = await q2(db);
    console.log("q2:");
    console.log(query);

    query = await q3(db);
    console.log("q3:");
    console.log(query);

    query = await q4(db);
    console.log("q4:");
    console.log(query);

    query = await q5(db);
    console.log("q5:");
    console.log(query);

    cursor = await q6(db);
    console.log("q6:");
    console.log(cursor.length);

    cursor = await q7(db);
    console.log("q7:");
    console.log(cursor);
    
    cursor = await q8(db);
    console.log("q8:");
    console.log(cursor);

    cursor = await q9(db);
    console.log("q9:");
    console.log(cursor);

    cursor = await q10(db);
    console.log("q10:");
    console.log(cursor);
  } catch (err) {
      console.error(err);
  } finally {
      client.close();
  }
};
execute()

// Queries
function q1(db){
  let cursor = db.collection('restaurants').findOne({name: "Caffe Dante"}, {restaurant_id: 1});
  return cursor;
}

function q2(db){
  let cursor = db.collection('restaurants').find({name: /.*Steak.*/}, {projection: {_id: 0, restaurant_id: 1, name: 1}})
    .toArray();
  return cursor;
}

function q3(db){
  let cursor = db.collection('restaurants').find({cuisine: {$in: ["Italian", "American"]}, borough: "Brooklyn"}, {projection: {_id: 0, name: 1}})
    .toArray();
    return cursor;
}

function q4(db){
    let query = [
    {
        "$match" : {
            "cuisine" : /.*America.*/
        }
    },
    {
        "$group" : {
            "_id" : "$borough",
            "totalCnt" : {
                "$sum" : 1
            }
        }
    },
    {
        "$project" : {
            "borough" : 1,
            "totalCnt" : 1
        }
    },
    {
        "$sort" : {
            "totalCnt" : -1
        }
    }];

    let cursor = db.collection('restaurants').aggregate(query).toArray();
    return cursor;
}

function q5(db){
  let query = [
    {
      "$match" : {
        "cuisine" : /.*Chinese.*/,
        "borough" : /.*Manhattan.*/
      }
    },
    {
      "$unwind" : "$grades"
    },
    {
      "$unwind" : "$grades.score"
    },
    {
      "$group" : {
        "_id" : "$_id",
        "totalScore" : {
          "$sum" : "$grades.score"
        },
        "name" : {
          "$first" : "$name"
        }
      }
    },
    {
      "$project" : {
        "name" : 1,
        "totalScore" : 1
      }
    },
    {
      "$sort" : {
        "totalScore" : -1
      }
    },
    {
      "$limit" : 5
    }];
    let cursor = db.collection('restaurants').aggregate(query).toArray();
    return cursor;
}

function q6(db){
  let query = [
      {
          "$unwind" : "$grades"
      },
      {
          "$unwind" : "$grades.score"
      },
      {
          "$match" : {
              "$and" : [
                  {
                      "address.coord" : {
                          "$geoWithin" : {
                              "$box" : [
                                  [
                                      -74,
                                      40.5
                                  ],
                                  [
                                      -73.5,
                                      40.7
                                  ]
                              ]
                          }
                      }
                  },
                  {
                      "grades.score" : {
                          "$gt" : 70
                      }
                  }
              ]
          }
      },
      {
          "$group" : {
              "_id" : "$_id"
          }
      },
      {
          "$project" : {
              "_id" : 1,
              "name" : 1
          }
      }
    ];
    let cursor = db.collection('restaurants').aggregate(query).toArray();
    return cursor;
}

function q7(db){
    let query = [
        {"$sort": {"pop": -1}},
        {"$project": {_id: 1, city: 1, state: 1}},
        {"$limit": 10}
    ];
    let cursor = db.collection('zips').aggregate(query).toArray();
    return cursor;
}

function q8(db){
    let query = [
        {"$group": {
            "_id": {"city": "$city", "state": "$state"},
            "pop": {"$sum": "$pop"},
            "state": {"$first": "$state"},
            "city": {"$first": "$city"}
        }},
        { "$sort": {"pop": -1}},
        {"$group": {
                "_id": "$state",
                "pop": {"$first": "$pop"},
                "city": {"$first": "$city"},
                "state": {"$first": "$state"}
            }
        },
        {"$project": {city: 1, state: 1, _id: 0}}
    ];
    let cursor = db.collection('zips').aggregate(query).toArray();
    return cursor
}

function q9(db){
    let query = [
        {"$group": {
            "_id": {"city": "$city", "state": "$state"},
            "pop": {"$sum": "$pop"},
            "state": {"$first": "$state"},
            "city": {"$first": "$city"}
        }},
        {"$group": {
                "_id": "$state",
                "avePop": {"$avg": "$pop"},
                "state": {"$first": "$state"},
                "totalPop": {"$sum": "$pop"}
            }
        },
        {"$match": {"avePop": {"$gt": 10000}}},
        {"$project": {
            state: 1, avePop: 1, _id: 0
        }}
    ];
    let cursor = db.collection('zips').aggregate(query).toArray();
    return cursor;
}

function q10(db){
    db.collection('zips').createIndex({loc:"2dsphere"});
    let cursor = db.collection('zips').find({
        loc:
          { $near :
             {
               $geometry: { type: "Point",  coordinates: [ -70, 40]},
               $maxDistance: 500000
             }
          }
      }, {projection: {_id: 0, city: 1}})
      .limit(5)
      .toArray();
    return cursor;
}

// MongoDB Shell Queries:
// 1: Find the restaurant ID of “Caffe Dante”.
// ```
// db.restaurants.findOne({name: "Caffe Dante"}, {restaurant_id: 1})
// ```

// 2: Find all restaurants whose name has “Steak” in it, return only the restaurant’s ids and names.
// ```
// db.restaurants.find({name: /.*Steak.*/}, {restaurant_id: 1, name: 1})
// ```

// 3: Find the names of all restaurants that serve either Italian or American cuisine and are located in the Brooklyn borough. (the result of cuisine is just "Italian" or "American")
// ```
// db.restaurants.find({cuisine: {$in: ["Italian", "American"]}, borough: "Brooklyn"}, {name: 1} )
// ```

// 4: Return the list of boroughs ranked by the number of American restaurants (cuisine with the word "America") in it. That is, for each borough, find how many restaurants serve American cuisine an print the borough and the number of such restaurants sorted by this number in descending order.
// ```
// query = [
//    {
//        "$match" : {
//            "cuisine" : /.*America.*/
//        }
//    },
//    {
//        "$group" : {
//            "_id" : "$borough",
//            "totalCnt" : {
//                "$sum" : 1
//            }
//        }
//    },
//    {
//        "$project" : {
//            "borough" : 1,
//            "totalCnt" : 1
//        }
//    },
//    {
//        "$sort" : {
//            "totalCnt" : -1
//        }
//    }
// ]
// db.restaurants.aggregate(query)
// ```

// 5: Find the top 5 Chinese restaurants (cuisine with the word "Chinese") in Manhattan that have the highest total score. Return for each restaurant the restaurants’ name and the total score. Hint: You can use “$unwind”.
// ```
// query = [
// 	{
// 		"$match" : {
// 			"cuisine" : /.*Chinese.*/,
// 			"borough" : /.*Manhattan.*/
// 		}
// 	},
// 	{
// 		"$unwind" : "$grades"
// 	},
// 	{
// 		"$unwind" : "$grades.score"
// 	},
// 	{
// 		"$group" : {
// 			"_id" : "$_id",
// 			"totalScore" : {
// 				"$sum" : "$grades.score"
// 			},
// 			"name" : {
// 				"$first" : "$name"
// 			}
// 		}
// 	},
// 	{
// 		"$project" : {
// 			"name" : 1,
// 			"totalScore" : 1
// 		}
// 	},
// 	{
// 		"$sort" : {
// 			"totalScore" : -1
// 		}
// 	},
// 	{
// 		"$limit" : 5
// 	}
// ]
// db.restaurants.aggregate(query)
// ```

// 6: Consider a rectangle area on the location field, in which the vertices are [ -74 , 40.5 ] , [ -74 , 40.7 ] , [ -73.5 , 40.5 ] and [ -73.5 , 40.7 ]. Find the number of restaurants in this area that have received a grade score (at least one) more than 70. Hint: Use the $geoWithin and $box.
// ```
// query = [
// 	{
// 		"$unwind" : "$grades"
// 	},
// 	{
// 		"$unwind" : "$grades.score"
// 	},
// 	{
// 		"$match" : {
// 			"$and" : [
// 				{
// 					"address.coord" : {
// 						"$geoWithin" : {
// 							"$box" : [
// 								[
// 									-74,
// 									40.5
// 								],
// 								[
// 									-73.5,
// 									40.7
// 								]
// 							]
// 						}
// 					}
// 				},
// 				{
// 					"grades.score" : {
// 						"$gte" : 70
// 					}
// 				}
// 			]
// 		}
// 	},
// 	{
// 		"$group" : {
// 			"_id" : "$_id"
// 		}
// 	},
// 	{
// 		"$project" : {
// 			"_id" : 1,
// 			"name" : 1
// 		}
// 	}
// ]
// db.restaurants.aggregate(query).toArray().length
// ```

// 7: Find the  top 10 zipcodes with the largest population, return the zipcode, the city name and the state
// ```
// db.zips.aggregate([
//     {"$sort": {"pop": -1}},
//     {"$project": {_id: 1, city: 1, state: 1}},
//     {"$limit": 10}
// ]);
// ```

// 8: Find the largest city in each state, return the city name and the state
// ```
// db.zips.aggregate([
//     {"$group": {
//         "_id": {"city": "$city", "state": "$state"},
//         "pop": {"$sum": "$pop"},
//         "state": {"$first": "$state"},
//         "city": {"$first": "$city"}
//     }},
//     { "$sort": {"pop": -1}},
//     {"$group": {
//             "_id": "$state",
//             "pop": {"$first": "$pop"},
//             "city": {"$first": "$city"},
//             "state": {"$first": "$state"}
//         }
//     },
//     {"$project": {city: 1, state: 1, _id: 0}}
// ]);
// ```

// 9: Find the states where the average population of cities is larger than 10000, return the population and the state
// ```
// db.zips.aggregate([
//     {"$group": {
//         "_id": {"city": "$city", "state": "$state"},
//         "pop": {"$sum": "$pop"},
//         "state": {"$first": "$state"},
//         "city": {"$first": "$city"}
//     }},
//     {"$group": {
//             "_id": "$state",
//             "avePop": {"$avg": "$pop"},
//             "state": {"$first": "$state"},
//             "totalPop": {"$sum": "$pop"}
//         }
//     },
//     {"$match": {"avePop": {"$gt": 10000}}},
//     {"$project": {
//         state: 1, totalPop: 1, _id: 0
//     }}
// ]);
// ```

// 10: Find the top 5 cities nearest to  [-70,40], return only the city name. Hint: Use the $near.

// ```
// db.zips.createIndex({loc:"2dsphere"});
// db.zips.find(
//   {
//     loc:
//       { $near :
//           {
//             $geometry: { type: "Point",  coordinates: [ -70, 40]},
//             $maxDistance: 200000
//           }
//       }
//   }
// )
// ```