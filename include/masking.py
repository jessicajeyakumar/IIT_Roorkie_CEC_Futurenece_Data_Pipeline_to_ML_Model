def mask_email(email):
    if not isinstance(email, str) or '@' not in email:
        return "invalid@masked.com"
    user, domain = email.split('@', 1)
    return f"{user[0]}***@{domain}" if len(user) > 1 else f"*@{domain}"

def mask_phone(phone):
    phone_str = str(phone).strip()
    if len(phone_str) >= 10:
        return f"{phone_str[:2]}******{phone_str[-2:]}"
    return "**********"
