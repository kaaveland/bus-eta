import logging
from kollektivkart import webapp

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    webapp.app.run_server(debug=True)
