import { connect, close } from './connection.js';

const db = await connect();
const usersCollection = db.collection("users");
//const articlesCollection = db.collection("articles");
const studentsCollection = db.collection("students");
const articlesName = 'articles';
//just tyr for first task1 using pipeline
const pipelineTask1 = [
  { $sort: { age: 1 } },
  { $limit: 5 },
  {
    $project: {
      firstName: 1,
      lastName: 1,
      age: 1,
      _id: 0,
    },
  },
];

const pipelineTask10 = [
  {
    $unwind: '$scores',
  },
  {
    $match: {
      'scores.type': 'homework',
    },
  },
  {
    $sort: {
      'scores.score': 1,
    },
  },
  {
    $group: {
      _id: '$_id',
      name: { $first: '$name' },
      worst_homework_score: { $first: '$scores.score' },
    },
  },
  {
    $project: {
      _id: 0,
    },
  },
  {
    $sort: {
      worst_homework_score: 1,
    },
  },
  {
    $limit: 1, 
  },
];

const pipelineTask11 = [
  {
    $unwind: '$scores',
  },
  {
    $match: {
      'scores.type': 'homework',
    },
  },
  {
    $group: {
      _id: null,
      avg_score: { $avg: '$scores.score' },
    },
  },
  {
    $project: {
      _id: 0,
    },
  },
]

const pipelineTask12 = [
  {
    $unwind: '$scores',
  },
  {
    $group: {
      _id: '$_id',
      name: { $first: '$name' },
      avg_scores: { $avg: '$scores.score' },
    },
  },
  {
    $sort: {
      avg_scores: -1,
    },
  },
];

const run = async () => {
  try {
    await getUsersExample();
    //await task1();
    //await task2();
    // await task3();
    //await task4();
    // await task5();
    // await task6();
    // await task7();
    // await task8();
    // await task9();
    // await task10();
    // await task11();
    // await task12();

    await close();
  } catch(err) {
    console.log('Error: ', err)
  }
}
run();



// #### Users
// - Get users example
async function getUsersExample () {
  try {
    const [allUsers, firstUser] = await Promise.all([
      usersCollection.find().toArray(),
      usersCollection.findOne(),
    ])

    console.log('allUsers', allUsers);
    console.log('firstUser', firstUser);
  } catch (err) {
    console.error('getUsersExample', err);
  }
}

// - Get all users, sort them by age (ascending), and return only 5 records with firstName, lastName, and age fields.
async function task1 () {
  try {
    const result = await usersCollection.aggregate(pipelineTask1, { maxTimeMS: 60000, allowDiskUse: true }).toArray();
    let task1 = await usersCollection.find({})
      .project({ firstName: 1, lastName: 1, age: 1, _id: 0 })
      .sort({ age: 1 })
      .limit(5)
      .toArray();
    console.log('Task 1 using aggregation:', result);
    console.log('Task 1:', task1);
  } catch (err) {
    console.error('task1', err)
  }
}

// - Add new field 'skills: []" for all users where age >= 25 && age < 30 or tags includes 'Engineering'
async function task2 () {
  try {
    const updateResult = await usersCollection.updateMany(
      {
        $or: [
          { age: { $gte: 25, $lt: 30 } },
          { tags: { $in: ['Engineering'] } },
        ],
      },
      {
        $set: { skills: [] },
      }
    );
    const updatedUsers = await usersCollection.find({}).toArray()
    console.error('Task 2:', updatedUsers)
  } catch (err) {
    console.error('task2', err)
  }
}

// - Update the first document and return the updated document in one operation (add 'js' and 'git' to the 'skills' array)
//   Filter: the document should contain the 'skills' field
async function task3() {
  try {
    const filter = { skills: { $exists: true } };
    const update = {
      $addToSet: { skills: { $each: ['js', 'git'] } },
    };
    
    const options = { returnDocument: 'after' };
    const updatedDocument = await usersCollection.findOneAndUpdate(filter, update, options);
   
    console.log('Task 3:', updatedDocument);
  } catch (err) {
    console.error('task3', err)
  }
}

// - REPLACE the first document where the 'email' field starts with 'john' and the 'address state' is equal to 'CA'
//   Set firstName: "Jason", lastName: "Wood", tags: ['a', 'b', 'c'], department: 'Support'
async function task4 () {
  try {
    const filterTask4 = { email: /^john/i, 'address.state': 'CA' };
    const updateTask4 = {
      $set: {
        firstName: 'Jason',
        lastName: 'Wood',
        tags: ['a', 'b', 'c'],
        department: 'Support',
      },
    };

    const result = await usersCollection.updateOne(filterTask4, updateTask4);
    
    console.log('Task 4:', result);
  } catch (err) {
    console.log('task4', err);
  }
}

// - Pull tag 'c' from the first document where firstName: "Jason", lastName: "Wood"
async function task5 () {
  try {
    const filterTask5 = { firstName: 'Jason', lastName: 'Wood' };
    const updateTask5 = { $pull: { tags: 'c' } };
    const updateResultTask5 = await usersCollection.updateOne(filterTask5, updateTask5);
    console.log('Task 5:', updateResultTask5);
  } catch (err) {
    console.log('task5', err);
  }
}

// - Push tag 'b' to the first document where firstName: "Jason", lastName: "Wood"
//   ONLY if the 'b' value does not exist in the 'tags'
async function task6 () {
  try {
    const filterTask6 = { firstName: 'Jason', lastName: 'Wood' };
    const updateTask6 = { $addToSet: { tags: 'b' } };

    const updateResultTask6 = await usersCollection.updateOne(filterTask6, updateTask6);

    console.log('Task 6:', updateResultTask6);
  } catch (err) {
    console.log('task6', err);
  }
}

// - Delete all users by department (Support)
async function task7 () {
  try {
    const deleteResultTask7 = await usersCollection.deleteMany({ department: 'Support' });

    console.log('Task 7:', deleteResultTask7);
  } catch (err) {
    console.log('task7', err);
  }
}

// #### Articles
// - Create new collection 'articles'. Using bulk write:
//   Create one article per each type (a, b, c)
//   Find articles with type a, and update tag list with next value ['tag1-a', 'tag2-a', 'tag3']
//   Add tags ['tag2', 'tag3', 'super'] to articles except articles with type 'a'
//   Pull ['tag2', 'tag1-a'] from all articles
async function task8 () {
  try {
    // Create the new collection  
    const articlesCollection = await db.createCollection(articlesName);

    console.log(`Collection '${articlesName}' created successfully.`);

    //Create one article per each type (a, b, c)
    const articlesToInsert = [
      { type: 'a' },
      { type: 'b' },
      { type: 'c' }
    ];
    const insertResult = await articlesCollection.insertMany(articlesToInsert);
    console.log('Task 8. Articles 1:', insertResult);

    //Find articles with type 'a' and update the tag list
    const filterArticles2 = { type: 'a' };
    const updateArticles2 = { $set: { tags: ['tag1-a', 'tag2-a', 'tag3'] } };

    const updateResultArticles2 = await articlesCollection.updateMany(filterArticles2, updateArticles2);
    console.log('Task 8. Articles 2:', updateResultArticles2);

    // Add tags ['tag2', 'tag3', 'super'] to articles except type 'a'
    const filterArticles3 = { type: { $ne: 'a' } };
    const updateArticles3 = { $addToSet: { tags: { $each: ['tag2', 'tag3', 'super'] } } };

    const updateResultArticles3 = await articlesCollection.updateMany(filterArticles3, updateArticles3);
    console.log('Task 8. Articles 3:', updateResultArticles3);

    // Pull ['tag2', 'tag1-a'] from all articles
    const filterArticles4 = {};
    const updateArticles4 = { $pull: { tags: { $in: ['tag2', 'tag1-a'] } } };

    const updateResultTask4 = await articlesCollection.updateMany(filterArticles4, updateArticles4);
    console.log('Task 8. Articles 4:', updateResultTask4);

  } catch (err) {
    console.error('task8', err);
  }
}

// - Find all articles that contains tags 'super' or 'tag2-a'
async function task9 () {
  try {
    const filterTask9 = { tags: { $in: ['super', 'tag2-a'] } };

    const articlesWithTags = await articlesCollection.find(filterTask9).toArray();
    console.log('Task 9:', articlesWithTags);

  } catch (err) {
    console.log('task9', err);
  }
}

// #### Students Statistic (Aggregations)
// - Find the student who have the worst score for homework, the result should be [ { name: <name>, worst_homework_score: <score> } ]
async function task10 () {
  try {
    const result = await studentsCollection.aggregate(pipelineTask10).toArray();
    console.log('Student with the worst homework score:', result);

  } catch (err) {
    console.log('task10', err);
  } 
}

// - Calculate the average score for homework for all students, the result should be [ { avg_score: <number> } ]
async function task11 () {
  try {
    const result = await studentsCollection.aggregate(pipelineTask11).toArray();
    console.log('Average score for homework for all students:', result);
  } catch (err) {
    console.log('task11', err);
  } 
}

// - Calculate the average score by all types (homework, exam, quiz) for each student, sort from the largest to the smallest value
async function task12 () {
  try {
    const result = await studentsCollection.aggregate(pipelineTask12).toArray();
    console.log('Average score for each student by all types (sorted):', result);

  } catch (err) {
    console.log('task12', err);
  } 
}
