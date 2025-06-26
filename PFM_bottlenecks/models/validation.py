from pydantic import Field, BaseModel
from typing import Optional
from .base import ConfidenceLevel

class BottleneckValidation_1_1(BaseModel):
    is_too_general: Optional[bool] = Field(
        None,
        description="True if the extracted evidence is vague or lacks specific, grounded examples."
    )
    no_commitment_signal: Optional[bool] = Field(
        None,
        description="True if the evidence doesn't mention (or imply) lack of political/technical commitment to action."
    )
    only_resource_mention: Optional[bool] = Field(
        None,
        description="True if the statement only discusses funding or resources without indicating leadership inaction or resistance."
    )
    fits_other_bottleneck: Optional[bool] = Field(
        None,
        description="True if the content appears more relevant to a different bottleneck (e.g. coordination, resource mismatch)."
    )
    suggested_bottleneck: Optional[str] = Field(
        None,
        description="Optional short label for what bottleneck this seems to be about, if not 1.1."
    )
    reform_intent_without_followthrough: Optional[bool] = Field(
        None,
        description="True if the text shows reform ideas or plans without evidence of political commitment to follow through."
    )
    international_example_only: Optional[bool] = Field(
        None,
        description="True if the evidence refers only to another country or generic experience, not local context."
    )
    about_stakeholder_engagement: Optional[bool] = Field(
        None,
        description="True if the chunk is about consultation or stakeholder dialogue, not commitment."
    )
    validation_reasoning: Optional[str] = Field(
        None,
        description="Explanation justifying the validation decision."
    )
    confidence: Optional[ConfidenceLevel] = Field(
        None,
        description="Confidence level that the extracted evidence supports the bottleneck. "
                    "Choose 'strong' for clear and direct evidence, 'borderline' for partial or ambiguous relevance, "
                    "and 'weak' for tenuous or indirect evidence."
    )

    is_valid: bool = Field(
        description="True if the extracted evidence strongly supports the bottleneck; False otherwise."
    )

    @classmethod
    def validation_guidance(cls) -> str:
        return """
        Validation Guidance for Bottleneck 1.1 – Inadequate Commitment of Political and Technical Leadership
        
        This bottleneck focuses on situations where key decision-makers—political or technical—fail to follow through on approved policy actions due to reluctance, resistance, or lack of sustained commitment.
        
        Reject evidence in the following situations:
        
        - Too general or vague — lacks detail or grounded examples.
        - No clear commitment signal — doesn't indicate resistance, inaction, or avoidance by leadership.
        - Only mentions funding — refers to financial needs without linking to leadership failure.
        - Better fits another bottleneck — for example, issues of coordination, implementation, or resourcing.
        - Describes stakeholder engagement — consultation does not imply lack of commitment.
        - Mentions reform ideas without follow-through — statements of intent without action.
        - Refers only to international examples — must include local context or a directly comparable situation.
        
        Valid evidence should:
        
        - Point to actions not being taken due to lack of political or technical will.
        - Indicate avoidance, delay, or quiet abandonment of policy goals by leadership.
        """


class BottleneckValidation_2_1(BaseModel):
    is_too_general: Optional[bool] = Field(
        None, description="True if the evidence is vague or lacks specific examples of fragmented or inconsistent policies."
    )
    no_fragmentation_signal: Optional[bool] = Field(
        None, description="True if the chunk does not mention disconnected, disjointed, uncoordinated, fragmented, incoherent, confused, or siloed policy formulation."
    )
    only_mentions_policy_quality: Optional[bool] = Field(
        None, description="True if the statement critiques policy quality or feasibility but not inconsistency incoherence, duplication, or overlap."
    )
    about_implementation_not_design: Optional[bool] = Field(
        None, description="True if the evidence focuses on implementation failures, not lack of coordination, incoherence or duplication in policy design."
    )
    about_donor_influence_but_not_fragmentation: Optional[bool] = Field(
        None, description="True if the evidence references donor influence or support but not its role in fragmentation or duplication."
    )
    fits_other_bottleneck: Optional[bool] = Field(
        None, description="True if the content appears more relevant to another bottleneck."
    )
    suggested_bottleneck: Optional[str] = Field(
        None, description="Optional short label for what bottleneck this seems to be about, if not 2.1."
    )
    international_example_only: Optional[bool] = Field(
        None, description="True if the evidence refers only to international examples or generic experience, not the specific country in question."
    )
    validation_reasoning: Optional[str] = Field(
        None, description="Explanation justifying the validation decision."
    )
    confidence: Optional[ConfidenceLevel] = Field(
        None, description="Confidence level that the extracted evidence supports the bottleneck."
    )
    is_valid: bool = Field(
        description="True if the extracted evidence strongly supports the bottleneck; False otherwise."
    )

    @classmethod
    def validation_guidance(cls) -> str:
        return """
        Validation Guidance for Bottleneck 2.1 – Fragmented, Inconsistent and Uncoordinated Policies

        This bottleneck captures failures in policy coherence across or within sectors — where different policies contradict, overlap, or duplicate efforts due to weak coordination, because policymaking is fragmented across bureaucratic silos, due to management incentives or external donor support. These failures of policy coherence result in policies that are inconsistent and difficult to implement, or in unnecessary duplications and overlaps. 

        Reject evidence in the following situations:
        - Too vague or generic — lacks grounded, specific examples of fragmented or contradictory policy frameworks.
        - Only critiques ambition, feasibility, or quality — without pointing to cross-sector or intra-sectoral inconsistencies, incoherence, overlap or duplication.
        - Focuses on policy implementation rather than policy design — e.g., last-mile delivery problems.
        - Mentions management incentives but not their role in policy fragmentation or duplication.
        - Mentions donor involvement but not its role in policy fragmentation or duplication.
        - Misclassified — e.g., issues of funding (6.1), leadership commitment (1.1), or execution failure (6.3).
        - International-only example — must include or clearly apply to the specific ountry in question.

        Valid evidence should:
        - Identify inconsistent, disconnected, disjointed, uncoordinated, fragmented, incoherent, confused, overlapping, or duplicative government policies across or within sectors.
        - Show lack of coordination, misalignment of objectives, or contradictory mandates.
        - Reflect structural or institutional sources of fragmentation (e.g., bureaucratic silos, donor bypassing, management incentives). 
        """


class BottleneckValidation_3_1(BaseModel):
    is_too_general: Optional[bool] = Field(
        None, description="True if the evidence is vague or lacks concrete reference to revenue adequacy or revenue system structure."
    )
    only_mentions_funding_gaps: Optional[bool] = Field(
        None, description="True if the statement refers to underfunding or budget constraints without linking them to domestic revenue limitations."
    )
    about_expenditure_or_budget_not_revenue: Optional[bool] = Field(
        None, description="True if the chunk focuses on budgeting or spending levels rather than domestic system revenue system, i.e., tax base, revenue policy, revenue administration, and collection capabilities"
    )
    about_external_financing_not_domestic: Optional[bool] = Field(
        None, description="True if the evidence emphasizes external resources like aid or borrowing instead of domestic revenue."
    )
    fits_other_bottleneck: Optional[bool] = Field(
        None, description="True if the content appears more relevant to another bottleneck such as 5.2 (budget-policy disconnect), 1.1 (commitment), or 6.1 (fragmented funding)."
    )
    suggested_bottleneck: Optional[str] = Field(
        None, description="Optional short label for what bottleneck this seems to be about, if not 3.1."
    )
    international_example_only: Optional[bool] = Field(
        None, description="True if the evidence refers only to international or generic benchmarks and not the specific country in question."
    )
    validation_reasoning: Optional[str] = Field(
        None, description="Explanation justifying the validation decision."
    )
    confidence: Optional[ConfidenceLevel] = Field(
        None, description="Confidence level that the extracted evidence supports the bottleneck."
    )
    is_valid: bool = Field(
        description="True if the extracted evidence strongly supports the bottleneck; False otherwise."
    )

    @classmethod
    def validation_guidance(cls) -> str:
        return """
        Validation Guidance for Bottleneck 3.1 – Domestic Revenue Policies Generate Insufficient Resources to Achieve Policy Goals

        This bottleneck focuses on whether the **policy design or limited administration capabilities of domestic revenue systems** (i.e. covering tax base, revenue policy, revenue administration, or collection capabilities) constrain the government’s ability to fund its policy objectives.

        Reject evidence in the following situations:
        - Too vague or abstract — lacks reference to specific revenue constraints.
        - Mentions funding shortages or gaps without linking them to the **domestic revenue system** whether policy design or administration capability.
        - Focuses on spending or budgeting capacity, not features of the revenue system such as tax base, revenue policy, revenue adimnistration and collection capabilities.
        - Centers on external financing, grants, or borrowing, not domestic sources of revenue.
        - Fits better under another bottleneck (e.g., misalignment between budgets and policies, fragmented funding).
        - Refers only to international benchmarks or comparisons without relevance to the specific country in question.

        Valid evidence should:
        - Refer to **low domestic revenue mobilization** (e.g., low tax-to-GDP, narrow base, under performance of revenue collection).
        - Mention factors that **limit domestic revenue collection related to tax base, revenue policy, revenue administration, or collection capabilities**. 
.
        - Highlight misalignment between revenue potential and policy ambition or between revenue potential and revenue collection. 
        """


class BottleneckValidation_6_1(BaseModel):
    is_too_general: Optional[bool] = Field(
        None, description="True if the evidence is vague or lacks clear reference to how funding mechanisms impede service delivery."
    )
    only_mentions_funding_levels: Optional[bool] = Field(
        None, description="True if the chunk mentions how much funding exists or is needed, without discussing fragmentation, delay, or unpredictability."
    )
    no_fragmentation_signal: Optional[bool] = Field(
        None, description="True if the chunk doesn’t mention multiple funding channels, parallel systems, or coordination failures."
    )
    no_reference_to_donors_or_parallel_systems: Optional[bool] = Field(
        None, description="True if donor-related fragmentation or use of parallel systems is not mentioned when expected."
    )
    not_about_funding: Optional[bool] = Field(
        None, description="True if the chunk discusses project execution, governance, or service delivery without reference to funding mechanisms at all."
    )
    only_about_volatility_or_variation: Optional[bool] = Field(
        None, description="True if the evidence mentions funding volatility, unpredictability, or variation without showing fragmented/ad hoc channels."
    )
    system_issues_without_fragmentation: Optional[bool] = Field(
        None, description="True if the text is about system capacity or PFM weaknesses, but not about fragmentation or multiple funding streams."
    )
    donor_dependency_without_fragmentation: Optional[bool] = Field(
        None, description="True if the statement refers to donor reliance but not to fragmentation or parallel structures."
    )
    mentions_donor_coordination_but_not_fragmentation: Optional[bool] = Field(
        None, description="True if it refers to donor alignment/policy coordination but lacks details on fragmented or duplicative systems."
    )
    refers_to_solution_or_reform_plan: Optional[bool] = Field(
        None, description="True if the text describes a reform or intended solution rather than identifying a problem."
    )
    international_example_only: Optional[bool] = Field(
        None, description="True if the example refers to a different country or region and not to the context under analysis."
    )
    fits_other_bottleneck: Optional[bool] = Field(
        None, description="True if the evidence seems to be about a different bottleneck (e.g., implementation failure, capacity issues, delays)."
    )
    suggested_bottleneck: Optional[str] = Field(
        None, description="Short label of another bottleneck this chunk might better support, if not 6.1."
    )
    validation_reasoning: Optional[str] = Field(
        None,
        description="Explanation justifying the validation decision."
    )
    confidence: Optional[ConfidenceLevel] = Field(
        None,
        description="Confidence level that the extracted evidence supports the bottleneck. "
                    "Choose 'strong' for clear and direct evidence, 'borderline' for partial or ambiguous relevance, "
                    "and 'weak' for tenuous or indirect evidence."
    )

    is_valid: bool = Field(
        description="True if the extracted evidence strongly supports the bottleneck; False otherwise."
    )

    @classmethod
    def validation_guidance(cls) -> str:
        return """
    Validation Guidance for Bottleneck 6.1 – Ad hoc, Political and Fragmented Funding Channels
    
    This bottleneck focuses on whether **the structure and fragmentation of funding channels** undermines effective service or project delivery. Your task is to assess whether the evidence shows a problem arising from multiple, disconnected, or politically influenced funding streams — particularly if this causes inefficiencies, uncertainty, or administrative burden.
    
    Reject evidence in the following situations:
    
    - Too general or vague — no specific examples of funding systems or fragmentation.
    - Describes volatility or unpredictability of funding, but not fragmentation or structural issues.
    - Refers to shortfalls or underfunding without specifying the fragmented or parallel nature of financing.
    - Discusses unreliability or late disbursements, but not political discretion or multiple funding streams.
    - Focuses on donor dependency without addressing parallel or disconnected systems.
    - Describes weaknesses in systems (e.g., poor controls or procedures) without reference to fragmentation or parallelism.
    - Mentions solutions or reforms already underway, rather than identifying a bottleneck.
    - Cites international experiences without clear relevance to the context being assessed.
    - Addresses coordination failures without linking them to fragmented funding mechanisms.
    
    Valid evidence should describe:
    
    - The presence of **multiple, disconnected, or parallel funding mechanisms** (e.g. budget funds, donor funds, earmarked transfers).
    - Examples of **political discretion** influencing fund allocation or availability.
    - **Administrative inefficiencies or confusion** caused by having multiple reporting or budgeting systems.
    - Specific instances where **fragmentation of funding** made policy planning or implementation difficult.
    """
