
def make_bottleneck_prompt(
    text: str,
    role_of_public_finance: str,
    role_description: str,
    challenge_name: str,
    challenge_description: str,
    bottleneck_name: str,
    bottleneck_description: str) -> str:
    
    return f"""
        You are analyzing a public finance document to identify specific bottlenecks affecting development outcomes.
        
        The context for your analysis is as follows:
        
        Role of Public Finance: {role_of_public_finance}
        → {role_description}
        
        PFM Challenge: {challenge_name}
        → {challenge_description}
        
        Specific Bottleneck: {bottleneck_name}
        → {bottleneck_description}
        
        ---
        
        Your task:
        
        - Carefully read the excerpt below.
        - Extract direct evidence from the text that clearly supports the presence of the specific bottleneck listed above.
        - Only extract text that is explicitly present in the excerpt.
        - Do not infer, assume, or include information that is not stated.
        - If you find no clear evidence, return null.
        
        For each piece of extracted evidence, briefly explain your reasoning (i.e., why this excerpt indicates the bottleneck), and indicate if the match is ambiguous.
        
        Text to analyze:
        
        {text}
        """

def make_validation_prompt(
    extracted_evidence: str,
    reasoning: str,
    role_of_public_finance: str,
    role_description: str,
    challenge_name: str,
    challenge_description: str,
    bottleneck_name: str,
    bottleneck_description: str,
    validation_model_cls=None 
) -> str:
    # Use class-based guidance if available
    bottleneck_specific_criteria = ""
    if validation_model_cls and hasattr(validation_model_cls, "validation_guidance"):
        bottleneck_specific_criteria = validation_model_cls.validation_guidance()

    return f"""
        You are validating whether a previously extracted piece of text is strong evidence of a specific bottleneck in public finance.
        
        Here is the context:
        
        Role of Public Finance: {role_of_public_finance}
        → {role_description}
        
        PFM Challenge: {challenge_name}
        → {challenge_description}
        
        Specific Bottleneck: {bottleneck_name}
        → {bottleneck_description}
        
        {f"Additional evaluation criteria: {bottleneck_specific_criteria}" if bottleneck_specific_criteria else ""}
        
        ---
        
        Extracted Evidence: {extracted_evidence}
        
        Reasoning for Extraction:
        {reasoning}
        
        ---
        
        Your task:
        
        - First, evaluate whether the evidence is general, misplaced, insufficient, or misclassified using the bottleneck-specific criteria above.
        - Then, reflect on those evaluations to decide:
            - Does this evidence clearly and directly support the bottleneck?
            - Is the reasoning plausible and grounded in the evidence?
        - If the evidence is vague, general, or fits another bottleneck better, mark `is_valid` as False.
        - Ensure the `is_valid` field reflects your judgment based on the flags you select.
        - Your validation reasoning must explain how the intermediate evaluations informed your final decision.
        """