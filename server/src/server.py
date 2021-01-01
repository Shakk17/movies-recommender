import os
from multiprocessing.dummy import Process

from flask import Flask, request

from helpers.helper import refine_db, show_predictions, update_my_ratings
from model import train_model
from run_crawler import run_imdb_crawler

app = Flask('server')


def update_db():
    run_imdb_crawler()
    refine_db()
    train_model()


@app.route('/')
def index():
    return 'Ciaone'


@app.route('/show')
def show():
    return show_predictions()


# Updates the database and train model.
@app.route('/update', methods=['GET', 'POST'])
def update():
    if request.args.get('pwd') != os.environ['pwd']:
        return "Access denied."
    process = Process(target=update_db, args=())
    process.start()
    return "Updating database..."


# Train model.
@app.route('/train', methods=['GET', 'POST'])
def train():
    if request.args.get('pwd') != os.environ['pwd']:
        return "Access denied."
    process = Process(target=train_model, args=())
    process.start()
    return "Training model..."


# Updates my ratings.
@app.route('/update-ratings', methods=['GET', 'POST'])
def update_ratings():
    if request.args.get('pwd') != os.environ['pwd']:
        return "Access denied."
    process = Process(target=update_my_ratings, args=())
    process.start()
    return "Updating my ratings..."


# run the app
if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=os.environ.get('PORT', '5000'))
