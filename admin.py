from flask import Blueprint, render_template, request, redirect, url_for
from flask_login import login_required, current_user
from models import User, Subscription, db
from datetime import datetime, timedelta

admin_bp = Blueprint('admin', __name__)

ADMIN_EMAIL = "youremail@example.com"  # customize

@admin_bp.route('/admin', methods=['GET', 'POST'])
@login_required
def admin_dashboard():
    if current_user.email != ADMIN_EMAIL:
        return "Acesso restrito", 403

    if request.method == 'POST':
        email = request.form['email']
        cpf = request.form['cpf']
        days = int(request.form['days'])

        user = User.query.filter_by(email=email).first()
        if not user:
            return f"Usuário não encontrado: {email}", 400

        sub = Subscription(
            cpf=cpf,
            valid_until=datetime.utcnow() + timedelta(days=days),
            user_id=user.id
        )
        db.session.add(sub)
        db.session.commit()
        return redirect(url_for('admin.admin_dashboard'))

    users = User.query.all()
    return render_template('admin.html', users=users, now=datetime.utcnow())

