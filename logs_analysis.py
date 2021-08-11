#! /usr/bin/env python3
import psycopg2


# Create a view for top_articles
top_articles = "CREATE VIEW top_articles_view AS "\
                 "SELECT title, count(*) AS views "\
                 "FROM log JOIN articles "\
                 "ON log.path = concat('/article/', articles.slug) "\
                 "GROUP BY articles.title "\
                 "ORDER BY views DESC;"

# Select the top 3 popular articles from top_articles_view
top3_articles = "SELECT title, views FROM top_articles_view LIMIT 3;"

# Select popular authors
popular_authors = "SELECT authors.name AS name, "\
                    "sum(top_articles_view.views) AS author_views "\
                  "FROM top_articles_view JOIN authors "\
                  "ON authors.id = top_articles_view.author "\
                  "GROUP BY name ORDER BY author_views DESC;"

# Create an error view for log
errors = "CREATE VIEW error_log_view "\
           "SELECT date(TIME), "\
           "round(100.0 * sum(CASE log.status "\
           "WHEN '200 OK' THEN 0 ELSE 1 END) / count(log.status), 2) "\
           "AS \"Percent Error\" FROM log "\
           "GROUP BY date(TIME) ORDER BY \"Percent Error\" DESC;"

# Select days on which precent error is more than 1%
errors_log = "SELECT * FROM error_log_view WHERE \"Percent Error\" > 1;"


# Define a function to run queries with view
def execute_views():
    db = psycopg2.connect("dbname=news")
    cursor = db.cursor()
    cursor.execute(top_articles)
    cursor.execute(errors)
    db.commit()
    db.close()


# Define a function to format results
def print_results(queryResult, ending):
    for co in queryResult:
        print('\t' + str(co[0]) + ' --- ' + str(co[1]) + ' ' + ending)


# Run queries and fetch results
if __name__ == '__main__':
    db = psycopg2.connect("dbname=news")
    cursor = db.cursor()

    cursor.execute(top3_articles)
    print("\n1. The most popular three articles of all time are: ")
    print_results(cursor.fetchall(), 'views')

    cursor.execute(popular_authors)
    print("\n2. The most popular article authors of all time are: ")
    print_results(cursor.fetchall(), 'views')

    cursor.execute(errors_log)
    print("\n3. Days on which more than 1% of requests leading to errors: ")
    print_results(cursor.fetchall(), '%')
db.close()
