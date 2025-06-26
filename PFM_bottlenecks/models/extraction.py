
from pydantic import Field
from typing import Optional

class BottleneckBase(BaseModel):
    confidence: Optional[ConfidenceLevel] = Field(
        None,
        description=(
            "How confidently the extracted evidence supports the bottleneck. "
            "Choose 'strong' if the evidence clearly and directly supports the bottleneck, "
            "'borderline' if it is somewhat relevant but may be open to interpretation, "
            "and 'weak' if the evidence is tenuous, ambiguous, or only indirectly related."
        )
    )

class Bottleneck_1_1(BottleneckBase):
    extracted_evidence: Optional[str] = Field(
        None,
        description=(
            "Verbatim excerpt from the text that provides concrete evidence of political or technical leadership failing to commit to implementing approved policies. "
            "Relevant evidence may include examples of delays, failure to act, resistance to disrupting the status quo, or refusal to reallocate resources despite stated goals. "
            "Mere descriptions of policy ambitions or general intentions are not sufficient. "
            "Focus only on failures of action, follow-through, or resource application. "
            "Use only direct text from the source; do not paraphrase or infer."
        )
    )
    reasoning: Optional[str] = Field(
        None,
        description=(
            "Short explanation of how the extracted text demonstrates weak or absent leadership commitment. "
            "The reasoning must be grounded entirely in the quoted text and explain how inaction, delay, or resistance is evident."
        )
    )

class Bottleneck_2_1(BottleneckBase):
    extracted_evidence: Optional[str] = Field(
        None,
        description=(
            "Verbatim excerpt from the text that provides concrete evidence of fragmented, inconsistent, or uncoordinated policy design. "
            "Look for examples of conflicting mandates, duplicative schemes, lack of alignment across sectors or institutions, or absence of cross-sector coordination mechanisms. "
            "Do not extract vague critiques of policy or general governance weakness without explicit reference to inter-policy inconsistency or siloed formulation. "
            "Use only direct text from the source; do not paraphrase or infer."
        )
    )
    reasoning: Optional[str] = Field(
        None,
        description=(
            "Brief explanation of how the extracted text illustrates fragmented or uncoordinated policy design. "
            "The reasoning should clarify why the excerpt demonstrates lack of alignment or duplication, and avoid interpretation beyond the quoted material."
        )
    )

class Bottleneck_3_1(BottleneckBase):
    extracted_evidence: Optional[str] = Field(
        None,
        description=(
            "Verbatim excerpt from the text that provides concrete evidence that the country’s domestic revenue policies or systems are structurally insufficient to support its development policy goals. "
            "This may include mention of low tax-to-GDP ratios, reliance on narrow tax bases, inability to mobilize adequate domestic resources, or systemic administrative limitations in tax collection. "
            "Do not extract general funding gaps, underbudgeting, or aspirational statements unless directly linked to the limitations of the revenue system. "
            "Use only direct text from the source; do not paraphrase or infer."
        )
    )
    reasoning: Optional[str] = Field(
        None,
        description=(
            "Short explanation of how the extracted text indicates a structural mismatch between revenue capacity and policy ambition. "
            "The reasoning must clearly tie the evidence to revenue system weaknesses rather than general resource constraints or spending shortfalls."
        )
    )


class Bottleneck_6_1(BottleneckBase):
    extracted_evidence: Optional[str] = Field(
        None,
        description=(
            "Verbatim excerpt from the text that shows financial resources for public service delivery are fragmented, delayed, diverted, or subject to political influences. "
            "Look for references to disconnected funding channels, bureaucratic bottlenecks, donor-specific procedures, funding unpredictability, or political interference. "
            "General dissatisfaction without explicit reference to financial flow issues should not be extracted. "
            "Use only direct text from the source; do not paraphrase or infer missing information."
        )
    )
    reasoning: Optional[str] = Field(
        None,
        description=(
            "Brief explanation of how the extracted text shows fragmentation, delay, or diversion of public financial resources. "
            "The reasoning should directly tie the evidence to problems in funding mechanisms or delivery channels."
        )
    )