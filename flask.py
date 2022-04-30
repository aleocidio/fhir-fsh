from Flask import (Flask, render_template, flash, request, jsonify, Markup)
import pandas as pd

@applicaton.before_first_request
def startup():
    df = pd.read_excel('recursos.xlsx')
    pass

@application.route('/background_process', methods=['POST', 'GET'])
def background_process():
    return jsonify({'resposta':'ok'})

@application.route('/', methods=['POST', 'GET'])
def index():
    
    return render_template('index.html' )
