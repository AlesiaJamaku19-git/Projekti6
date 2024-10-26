import json
from pymongo import MongoClient
from datetime import datetime

# Step 1: Connect to MongoDB
# Establish a connection to MongoDB running locally. 
# You can modify the connection string if using MongoDB Atlas or a different MongoDB server.
client = MongoClient("mongodb://localhost:27017/")  # Modify if using MongoDB Atlas
db = client['news_category_db']  # Create or connect to the database named 'news_category_db'


# Step 2: Load JSON Data and Insert into MongoDB
# Define a function to load data from a specified JSON file and insert it into a collection.
# Each entry in the JSON file is a news article containing fields like 'headline', 'category', 'authors', etc.
def insert_data(filename, collection_name):
    try:
        # Attempt to load the file as a JSON array
        with open(filename, 'r') as file:
            data = json.load(file)  # Load JSON data as an array of objects
            for entry in data:
                # Convert date to datetime object if available
                entry['date'] = datetime.strptime(entry['date'], '%Y-%m-%d') if entry.get('date') else None
            db[collection_name].insert_many(data)  # Insert all entries as a batch

    except json.JSONDecodeError:
        # If loading as an array fails, try line-by-line parsing
        print("JSON array load failed. Attempting line-by-line parsing.")
        with open(filename, 'r') as file:
            for line in file:
                try:
                    entry = json.loads(line.strip())  # Load each line as an individual JSON object
                    # Convert date to datetime object if available
                    entry['date'] = datetime.strptime(entry['date'], '%Y-%m-%d') if entry.get('date') else None
                    db[collection_name].insert_one(entry)  # Insert each entry one at a time
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line: {line}\nError: {e}")



# Specify the filename and collection mapping for inserting data
files_collections = {
    'News_Category_Dataset_v3.json': 'news'  # Example file: collection mapping
}

# Load and insert each file into the respective collection
for file, collection in files_collections.items():
    insert_data(file, collection)


# Step 3: Define Filtered Views
# Define a series of "views" that apply various filters to the data.
# Each view uses the aggregation pipeline in MongoDB to filter data based on different criteria.
def create_filter_views():
    # View 1: Filter articles in the 'ENTERTAINMENT' category
    # This retrieves only articles where the 'category' field is set to 'ENTERTAINMENT'.
    view1 = db.news.aggregate([{'$match': {'category': 'ENTERTAINMENT'}}])

    # View 2: Filter articles published between 2018 and 2022
    # Retrieves articles with a 'date' field between January 1, 2018, and December 31, 2022.
    view2 = db.news.aggregate([{'$match': {'date': {'$gte': datetime(2018, 1, 1), '$lte': datetime(2022, 12, 31)}}}])

    # View 3: Filter articles with headline length greater than 80 characters
    # Uses $expr and $strLenCP to calculate the length of the 'headline' field and apply the filter.
    view3 = db.news.aggregate([{'$match': {'$expr': {'$gt': [{'$strLenCP': "$headline"}, 80]}}}])

    # View 4: Filter articles with specific keywords in the short_description
    # Uses a regular expression to search for keywords like 'COVID' or 'pandemic' in the 'short_description' field.
    view4 = db.news.aggregate([{'$match': {'short_description': {'$regex': 'COVID|pandemic', '$options': 'i'}}}])

    # View 5: Filter articles authored by a specific author, e.g., "John Doe"
    # Uses a case-insensitive regular expression to match authors in the 'authors' field.
    view5 = db.news.aggregate([{'$match': {'authors': {'$regex': 'John Doe', '$options': 'i'}}}])

    # View 6: Filter articles with "Election" in the headline (case-insensitive)
    # Matches headlines that contain the word "Election", ignoring case.
    view6 = db.news.aggregate([{'$match': {'headline': {'$regex': 'Election', '$options': 'i'}}}])

    return [view1, view2, view3, view4, view5, view6]  # Return all views for further use


# Step 4: Define Aggregation Views
# Define a series of aggregation views for summarizing or transforming data.
def create_aggregation_views():
    # Aggregation View 1: Count articles per category
    # Groups documents by 'category' and counts the number of articles in each category.
    agg_view1 = db.news.aggregate([
        {'$group': {'_id': "$category", 'count': {'$sum': 1}}}
    ])

    # Aggregation View 2: Average short_description length per category
    # Calculates the average length of 'short_description' field in each category.
    agg_view2 = db.news.aggregate([
        {'$addFields': {'desc_length': {'$strLenCP': "$short_description"}}},
        {'$group': {'_id': "$category", 'average_desc_length': {'$avg': "$desc_length"}}}
    ])

    # Aggregation View 3: Count of articles per author
    # Groups documents by 'authors' and counts the number of articles for each author.
    agg_view3 = db.news.aggregate([
        {'$group': {'_id': "$authors", 'count': {'$sum': 1}}}
    ])

    # Aggregation View 4: Most frequent words in headlines
    # Splits headlines into individual words, then counts occurrences of each word and sorts by frequency.
    agg_view4 = db.news.aggregate([
        {'$project': {'words': {'$split': ["$headline", " "]}}},
        {'$unwind': '$words'},
        {'$group': {'_id': '$words', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 10}  # Limit to top 10 most frequent words
    ])

    # Aggregation View 5: Monthly count of articles
    # Groups documents by year and month of publication date, then counts articles for each period.
    agg_view5 = db.news.aggregate([
        {'$project': {'year_month': {'$dateToString': {'format': "%Y-%m", 'date': "$date"}}}},
        {'$group': {'_id': "$year_month", 'count': {'$sum': 1}}},
        {'$sort': {'_id': 1}}  # Sort by date ascending
    ])

    # Aggregation View 6: Earliest and latest publication date per category
    # Finds the earliest and latest date of publication for each category.
    agg_view6 = db.news.aggregate([
        {'$group': {
            '_id': "$category",
            'first_date': {'$min': "$date"},
            'last_date': {'$max': "$date"}
        }}
    ])

    return [agg_view1, agg_view2, agg_view3, agg_view4, agg_view5, agg_view6]  # Return all aggregations


# Step 5: Define Indexes to Optimize Queries
# Create indexes on fields frequently used in queries to improve query performance.
def create_indexes():
    # Index on 'category' for faster queries filtering by category
    db.news.create_index([("category", 1)])

    # Index on 'date' for efficient time-based queries
    db.news.create_index([("date", 1)])

    # Index on 'authors' for fast author-based filtering
    db.news.create_index([("authors", 1)])

    # Index on 'headline' for quicker text searches in headlines
    db.news.create_index([("headline", 1)])

    # Index on 'short_description' for efficient keyword searches in descriptions
    db.news.create_index([("short_description", 1)])

    # Compound index on 'category' and 'date' to optimize searches by category and date range
    db.news.create_index([("category", 1), ("date", 1)])


# Execute the Script
# Load data, create views, and set up indexes.
for file, collection in files_collections.items():
    insert_data(file, collection)

# Create filter views
create_filter_views()

# Create aggregation views
create_aggregation_views()

# Create indexes
create_indexes()
