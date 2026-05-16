BOOTSTRAP = "localhost:9092"

# BIAN-aligned topic taxonomy.
# Each topic carries the event produced by one Service Domain (SD)
# and is consumed by the next SD in the chain.
TOPIC_TRANSACTION_REQUESTED = "card.transaction.requested"   # from POS / Card Terminal
TOPIC_AUTHORIZATION_DECIDED = "card.authorization.decided"   # SD: Card Authorization
TOPIC_FRAUD_SCORED          = "card.fraud.scored"            # SD: Fraud Detection
TOPIC_FEE_CALCULATED        = "card.fee.calculated"          # SD: Card Fee Pricing
TOPIC_TRANSACTION_CLEARED   = "card.transaction.cleared"     # SD: Card Clearing
TOPIC_MERCHANT_SETTLED      = "merchant.settlement.posted"   # SD: Merchant Settlement

# Card networks recognised by the pipeline.
CARD_NETWORKS = ("VISA", "MASTERCARD", "AMEX", "DISCOVER")

# Card product tiers drive interchange pricing (issuer cost).
CARD_TIERS = ("DEBIT", "CREDIT_STANDARD", "CREDIT_REWARDS", "CREDIT_PREMIUM")

# Supported currencies; FX fees apply when txn currency != merchant settlement currency.
CURRENCIES = ("USD", "EUR", "GBP", "PKR", "JPY")

# Issuer / merchant country pool — cross-border fee applies when these differ.
COUNTRIES = ("US", "GB", "DE", "PK", "JP", "FR")
