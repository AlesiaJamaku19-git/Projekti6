import pandas as pd
import os

# Get the directory path where the script is executing
script_path = os.path.dirname(os.path.abspath(__file__))

# Move up one directory and specify the relative path to the target file
target_file_path = os.path.join(os.path.dirname(script_path), "Silver", "amazon-fine-food-reviews.csv")

print("Path of the file is :", target_file_path)

# Load your CSV file
data = pd.read_csv(target_file_path)


# 1. Product-Level Analysis
# Function to find the highest and lowest-rated products based on average score
def top_low_rated_products(df):
    # Ensure 'Time' column is converted to datetime to extract the year
    df['Year'] = pd.to_datetime(df['Time']).dt.year

    # Calculate average score for each product by year
    avg_scores = df.groupby(['ProductId', 'Year'])['Score'].mean().reset_index()
    
    # Find top 10 highest and lowest-rated products
    top_rated = avg_scores.nlargest(10, 'Score')
    low_rated = avg_scores.nsmallest(10, 'Score')
    
    # Save the results
    target_file_path = os.path.join(os.path.dirname(script_path), "Gold", "amazon-top-rated-products.csv")
    top_rated.to_csv(target_file_path, index=False)

    
    target_file_path = os.path.join(os.path.dirname(script_path), "Gold", "amazon-low-rated-products.csv")
    low_rated.to_csv(target_file_path, index=False)

# Function to compare products with the most reviews to their average scores
def popularity_vs_satisfaction(df):
    # Count number of reviews and calculate average score per product
    product_reviews = df.groupby('ProductId').agg(
        num_reviews=('Score', 'size'),
        avg_score=('Score', 'mean')
    ).reset_index()
    
    # Merge the average score back with the original DataFrame to keep Score column
    product_reviews = product_reviews.merge(df[['ProductId', 'Score']], on='ProductId', how='left')


    # Sort by number of reviews to see if highly reviewed products also have high ratings
    popular_products = product_reviews.sort_values(by='num_reviews', ascending=False)
    
    # Save the result
    target_file_path = os.path.join(os.path.dirname(script_path), "Gold", "amazon-popularity-vs-satisfaction.csv")
    popular_products.to_csv(target_file_path, index=False)

# Function to analyze if product ratings have changed over time
def product_improvements_over_time(df):
    # Convert 'Time' column to datetime
    df['Time'] = pd.to_datetime(df['Time'], unit='s')
    
    # Group by product and year, then calculate average rating per year
    df['Year'] = df['Time'].dt.year
    yearly_avg_score = df.groupby(['ProductId', 'Year'])['Score'].mean().reset_index()
    
    # Save the result
    target_file_path = os.path.join(os.path.dirname(script_path), "Gold", "amazon-product-improvements-over-time.csv")
    yearly_avg_score.to_csv(target_file_path, index=False)

# 2. Reviewer Behavior and User Engagement

# Function to identify top reviewers and analyze their average rating
def top_reviewers(df):
    # Count reviews per user and calculate their average rating
    user_reviews = df.groupby('UserId').agg(
        num_reviews=('Score', 'size'),
        avg_score=('Score', 'mean')
    ).reset_index()
    
    # Sort by number of reviews to identify top reviewers
    top_users = user_reviews.sort_values(by='num_reviews', ascending=False)
    
    # Save the result
    target_file_path = os.path.join(os.path.dirname(script_path), "Gold", "amazon-top-reviewers.csv")
    top_users.to_csv(target_file_path, index=False)

# Function to analyze helpfulness voting patterns of users
def helpfulness_voting_patterns(df):
    # Calculate total helpful votes per user
    user_helpfulness = df.groupby('UserId').agg(
        total_helpfulness_numerator=('HelpfulnessNumerator', 'sum'),
        total_helpfulness_denominator=('HelpfulnessDenominator', 'sum')
    ).reset_index()
    
    # Add helpfulness ratio
    user_helpfulness['helpfulness_ratio'] = (
        user_helpfulness['total_helpfulness_numerator'] / user_helpfulness['total_helpfulness_denominator']
    )
    
    # Save the result
    target_file_path = os.path.join(os.path.dirname(script_path), "Gold", "amazon-helpfulness-voting-patterns.csv")
    user_helpfulness.to_csv(target_file_path, index=False)

# 3. Helpfulness Analysis of Reviews

# Function to calculate helpfulness ratio and analyze factors contributing to high helpfulness
def helpfulness_ratio_analysis(df):
    # Calculate helpfulness ratio for each review
    df['helpfulness_ratio'] = df['HelpfulnessNumerator'] / df['HelpfulnessDenominator']
    
    # Drop rows where denominator is zero to avoid division errors
    helpful_reviews = df[df['HelpfulnessDenominator'] > 0]
    
    # Save the result
    target_file_path = os.path.join(os.path.dirname(script_path), "Gold", "amazon-helpfulness-ratio-analysis.csv")
    helpful_reviews.to_csv(target_file_path, index=False)

# 4. Temporal Analysis

# Function to analyze trends in review volume and scores over time
def trend_analysis_over_time(df):
    # Convert 'Time' to datetime
    df['Time'] = pd.to_datetime(df['Time'], unit='s')
    
    # Group by year and calculate the number of reviews and average score per year
    df['Year'] = df['Time'].dt.year
    yearly_trends = df.groupby('Year').agg(
        num_reviews=('Score', 'size'),
        avg_score=('Score', 'mean')
    ).reset_index()
    
    # Save the result
    target_file_path = os.path.join(os.path.dirname(script_path), "Gold", "amazon-trend-analysis-over-time.csv")
    yearly_trends.to_csv(target_file_path, index=False)

# Function to identify seasonal popularity of products
def seasonal_popularity(df):
    # Convert 'Time' to datetime
    df['Time'] = pd.to_datetime(df['Time'], unit='s')
    
    # Group by month and calculate number of reviews per month
    df['Month'] = df['Time'].dt.month
    monthly_trends = df.groupby('Month').size().reset_index(name='num_reviews')
    
    # Save the result
    target_file_path = os.path.join(os.path.dirname(script_path), "Gold", "amazon-seasonal-popularity.csv")
    monthly_trends.to_csv(target_file_path, index=False)

# 5. Exploring Anomalies and Biases

# Function to detect anomalies in ratings
def detect_rating_anomalies(df):
    # Group by ProductId to find mean and standard deviation of scores for each product
    product_stats = df.groupby('ProductId')['Score'].agg(['mean', 'std']).reset_index()
    product_stats = product_stats.rename(columns={'mean': 'avg_score', 'std': 'score_std_dev'})
    
    # Save the result
    target_file_path = os.path.join(os.path.dirname(script_path), "Gold", "amazon-detect-rating-anomalies.csv")
    product_stats.to_csv(target_file_path, index=False)

# Function to check for consistency in user ratings
def consistency_in_user_ratings(df):
    # Calculate average rating given by each user and the overall average rating of all reviews
    user_avg_score = df.groupby('UserId')['Score'].mean().reset_index(name='user_avg_score')
    overall_avg_score = df['Score'].mean()
    
    # Calculate difference from the overall average to spot biases
    user_avg_score['bias'] = user_avg_score['user_avg_score'] - overall_avg_score
    
    # Save the result
    target_file_path = os.path.join(os.path.dirname(script_path), "Gold", "amazon-consistency-in-user-ratings.csv")
    user_avg_score.to_csv(target_file_path, index=False)

# Call functions to run analysis and save results
top_low_rated_products(data)
popularity_vs_satisfaction(data)
product_improvements_over_time(data)
top_reviewers(data)
helpfulness_voting_patterns(data)
helpfulness_ratio_analysis(data)
trend_analysis_over_time(data)
seasonal_popularity(data)
detect_rating_anomalies(data)
consistency_in_user_ratings(data)
