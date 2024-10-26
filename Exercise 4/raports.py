import pandas as pd
import os 
import seaborn as sns
import matplotlib.pyplot as plt



# Get the directory path where the script is executing
script_path = os.path.dirname(os.path.abspath(__file__))


# Load the previously saved CSV files From Exercise 1 Folder Gold
top_rated_products = pd.read_csv(os.path.join(os.path.dirname(script_path), "Exercise 1", "Gold", "amazon-top-rated-products.csv"))
low_rated_products = pd.read_csv(os.path.join(os.path.dirname(script_path), "Exercise 1", "Gold", "amazon-low-rated-products.csv"))
popularity_vs_satisfaction = pd.read_csv(os.path.join(os.path.dirname(script_path), "Exercise 1", "Gold", "amazon-popularity-vs-satisfaction.csv"))
yearly_avg_score = pd.read_csv(os.path.join(os.path.dirname(script_path), "Exercise 1", "Gold", "amazon-product-improvements-over-time.csv"))
top_reviewers = pd.read_csv(os.path.join(os.path.dirname(script_path), "Exercise 1", "Gold", "amazon-top-reviewers.csv"))
monthly_trends = pd.read_csv(os.path.join(os.path.dirname(script_path), "Exercise 1", "Gold", "amazon-seasonal-popularity.csv"))
user_avg_score = pd.read_csv(os.path.join(os.path.dirname(script_path), "Exercise 1", "Gold", "amazon-consistency-in-user-ratings.csv"))
helpfulness_analysis = pd.read_csv(os.path.join(os.path.dirname(script_path), "Exercise 1", "Gold", "amazon-helpfulness-ratio-analysis.csv"))
yearly_trends = pd.read_csv(os.path.join(os.path.dirname(script_path), "Exercise 1", "Gold", "amazon-trend-analysis-over-time.csv"))


# 1. Line Plots
def line_plot_avg_score_over_time(df):
    """
    Line Plot showing the trend of average scores over the years.
    This helps to visualize how the average product rating has changed over time.
    """
    plt.figure(figsize=(10, 6))
    # yearly_avg_scores = df.groupby('Year')['Score'].mean().reset_index()
    # yearly_avg_scores.rename(columns={'Score': 'avg_score'}, inplace=True)
    sns.lineplot(data=df, x='Year', y='Score', marker='o')
    plt.title('Average Score Over Time')
    plt.xlabel('Year')
    plt.ylabel('Average Score')
    plt.grid()
    plt.savefig("amazon-line-plot-avg-score-over-time.png")
    plt.show()

def line_plot_num_reviews_over_time(df):
    """
    Line Plot showing the trend of the number of reviews over the years.
    This indicates the engagement or interest in the products over time.
    """
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='Year', y='num_reviews', marker='o')
    plt.title('Number of Reviews Over Time')
    plt.xlabel('Year')
    plt.ylabel('Number of Reviews')
    plt.grid()
    plt.savefig("amazon-line-plot-num-reviews-over-time.png")
    plt.show()

def line_plot_helpfulness_ratio(df):
    """
    Line Plot showing the trend of helpfulness ratio over the years.
    This indicates how helpful users find the reviews over time.
    """
    helpfulness_ratio = df.groupby('Year')['helpfulness_ratio'].mean().reset_index()
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=helpfulness_ratio, x='Year', y='helpfulness_ratio', marker='o')
    plt.title('Average Helpfulness Ratio Over Time')
    plt.xlabel('Year')
    plt.ylabel('Average Helpfulness Ratio')
    plt.grid()
    plt.savefig("amazon-line-plot-helpfulness-ratio-over-time.png")
    plt.show()


# 2. Bar Charts

def bar_chart_top_rated_products(df):
    """
    Bar Chart showing the top-rated products.
    This helps to quickly identify which products are most favored by users.
    """
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df, x='ProductId', y='Score', ci=None)
    plt.title('Top Rated Products')
    plt.xlabel('Product ID')
    plt.ylabel('Average Score')
    plt.xticks(rotation=45)
    plt.savefig("amazon-bar-chart-top-rated-products.png")
    plt.show()

def bar_chart_top_reviewers(df):
    """
    Bar Chart showing the top reviewers by number of reviews.
    This visualizes who the most active reviewers are within the dataset.
    """
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df.head(10), x='UserId', y='num_reviews', ci=None)
    plt.title('Top Reviewers by Number of Reviews')
    plt.xlabel('User ID')
    plt.ylabel('Number of Reviews')
    plt.xticks(rotation=45)
    plt.savefig("amazon-bar-chart-top-reviewers.png")
    plt.show()

def bar_chart_helpfulness_by_user(df):
    """
    Bar Chart showing average helpfulness ratio by user.
    This helps identify which users write the most helpful reviews on average.
    """
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df.sort_values(by='helpfulness_ratio', ascending=False).head(10), x='UserId', y='helpfulness_ratio', ci=None)
    plt.title('Top Users by Helpfulness Ratio')
    plt.xlabel('User ID')
    plt.ylabel('Average Helpfulness Ratio')
    plt.xticks(rotation=45)
    plt.savefig("amazon-bar-chart-helpfulness-by-user.png")
    plt.show()


# 3. Pie Charts

def pie_chart_top_products(df):
    """
    Pie Chart showing the proportion of top-rated products.
    This visualizes how many of the total reviews belong to the top-rated products.
    """
    top_products = df['ProductId'].value_counts().head(10)
    plt.figure(figsize=(8, 8))
    plt.pie(top_products, labels=top_products.index, autopct='%1.1f%%', startangle=140)
    plt.title('Top Products by Number of Reviews')
    plt.axis('equal')
    plt.savefig("amazon-pie-chart-top-products.png")
    plt.show()

def pie_chart_monthly_review_distribution(df):
    """
    Pie Chart showing the distribution of reviews by month.
    This helps to visualize which months see more engagement.
    """
    plt.figure(figsize=(8, 8))
    plt.pie(df['num_reviews'], labels=df['Month'], autopct='%1.1f%%', startangle=140)
    plt.title('Review Distribution by Month')
    plt.axis('equal')
    plt.savefig("amazon-pie-chart-monthly-review-distribution.png")
    plt.show()

def pie_chart_helpfulness_distribution(df):
    """
    Pie Chart showing the distribution of helpfulness ratios.
    This visualizes the proportion of reviews that users found helpful.
    """
    plt.figure(figsize=(8, 8))
    helpfulness_counts = pd.cut(df['helpfulness_ratio'], bins=[0, 0.5, 1, 1.5, 2], labels=['Low', 'Medium', 'High', 'Very High']).value_counts()
    plt.pie(helpfulness_counts, labels=helpfulness_counts.index, autopct='%1.1f%%', startangle=140)
    plt.title('Helpfulness Ratio Distribution')
    plt.axis('equal')
    plt.savefig("amazon-pie-chart-helpfulness-distribution.png")
    plt.show()


# 4. Other Chart Types

def box_plot_score_distribution(df):
    """
    Box Plot showing the distribution of scores.
    This visualizes the spread and outliers of review scores, highlighting any significant ratings.
    """
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x='Score')
    plt.title('Distribution of Review Scores')
    plt.xlabel('Score')
    plt.savefig("amazon-box-plot-score-distribution.png")
    plt.show()

# Call the visualization functions
line_plot_avg_score_over_time(yearly_avg_score)
line_plot_num_reviews_over_time(yearly_trends)
line_plot_helpfulness_ratio(helpfulness_analysis)

bar_chart_top_rated_products(top_rated_products)
bar_chart_top_reviewers(top_reviewers)
bar_chart_helpfulness_by_user(helpfulness_analysis)

pie_chart_top_products(popularity_vs_satisfaction)
pie_chart_monthly_review_distribution(monthly_trends)
pie_chart_helpfulness_distribution(helpfulness_analysis)

box_plot_score_distribution(popularity_vs_satisfaction)
