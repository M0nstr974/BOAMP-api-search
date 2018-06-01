from flask import Flask, render_template, request, redirect, url_for
import import_data

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"]) 
def homepage():
    if request.method == "POST":
        form_id = request.form['idAnnonce']
        form_nom = request.form['nom']
        form_commentaire = request.form['commentaire']
        form_pertinence = request.form['pertinence']

        bool_pertinence = True
        if form_pertinence == "nonPertinent":
            bool_pertinence = False

        import_data.update_annonce(form_id, bool_pertinence, form_nom, form_commentaire)
        
    return render_template("main.html")

@app.route("/import", methods=["GET", "POST"]) 
def importDaily():
    if request.method == "POST":
        redirect('/attente_import')
        import_data.get_daily_imports()
        return redirect('/')
    return render_template("import.html")

@app.route("/attente_import")
def attenteImport():
    return render_template("attente_import.html")

if __name__ == "__main__":
    app.run(debug = True)