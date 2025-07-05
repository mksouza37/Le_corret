# subscribe.py
import os
import stripe
from flask import Blueprint, redirect, url_for
from flask_login import login_required, current_user

subscribe_bp = Blueprint('subscribe', __name__)
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

@subscribe_bp.route('/subscribe')
@login_required
def create_checkout_session():
    domain_url = os.getenv("DOMAIN_URL", "http://localhost:5000")
    price_id = os.getenv("STRIPE_PRICE_ID")

    try:
        checkout_session = stripe.checkout.Session.create(
            success_url=domain_url + "/?session_id={CHECKOUT_SESSION_ID}",
            cancel_url=domain_url,
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{
                "price": price_id,
                "quantity": 1
            }],
            customer_email=current_user.email,
            metadata={
                "user_id": current_user.id
            }
        )
        return redirect(checkout_session.url)
    except Exception as e:
        return str(e), 400
