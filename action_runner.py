def run_actions(query: str, retrieved_text: str):
    return {
        "steps": [
            f"Step 1: Identify user intent → '{query}'.",
            "Step 2: Search SOP documents using simple keyword matching.",
            "Step 3: Found relevant information from the knowledge base.", # <-- UPDATED: Simplified text
            "Step 4: Format the answer in user’s requested language.",
            "Step 5: Return sources and reasoning steps."
        ]
    }