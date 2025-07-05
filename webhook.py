# webhook.py
import os
import stripe
from flask import Blueprint, request, jsonify
from models import db, User, Subscription
from datetime import datetime, timedelta

webhook_bp = Blueprint('webhook', __name__)
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")
endpoint_secret = os.getenv("STRIPE_WEBHOOK_SECRET")  # optional

@webhook_bp.route('/webhook', methods=['POST'])
def stripe_webhook():
    payload = request.data
    sig_header = request.headers.get("stripe-signature")

    try:
        if endpoint_secret:
            event = stripe.Webhook.construct_event(payload, sig_header, endpoint_secret)
        else:
            event = stripe.Event.construct_from(
                request.get_json(), stripe.api_key
            )
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        email = session.get('customer_email')
        user = User.query.filter_by(email=email).first()
        if user:
            sub = Subscription(
                cpf="000.000.000-00",  # placeholder
                valid_until=datetime.utcnow() + timedelta(days=365),
                stripe_id=session.get("id"),
                user_id=user.id
            )
            db.session.add(sub)
            db.session.commit()
        return jsonify({'status': 'success'})

    return jsonify({'status': 'ignored'})
