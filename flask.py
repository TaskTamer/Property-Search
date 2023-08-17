from flask import Flask, send_file

app = Flask(__name__)

# Scraper function to create SQLite database and insert sample data


@app.route('/api/get_database', methods=['GET'])
def get_database():
    return send_file("data.db", as_attachment=True)


if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0")
