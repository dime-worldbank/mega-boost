llm_model = 'gpt-4o'

pf_challenges = [
    {
        "challenge_id": 1,
        "role_of_public_finance": "Commitment to Feasible Policy",
        "role_description": (
            "Finance ministries and other central agencies play a fundamental coordinating role when it comes to policy decisions, "
            "functioning as a clearing house for various policy proposals across sector institutions, promoting dialogue and consultation around policy trade-offs, "
            "and ultimately linking policy objectives with resource availability, mobilization and use. "
            "This helps build commitment to policy decisions that are made and improves their feasibility and the likelihood that they will be implemented."
        ),
        "challenge_name": "Insufficient Stakeholder Commitment to Policy Action",
        "challenge_description": (
            "Focuses on whether political and technical stakeholders demonstrate sustained commitment to implementing approved policies, "
            "including challenges around ownership, continuity, and buy-in."
        ),
        "bottlenecks": [
            {
                "bottleneck_id": "1.1",
                "bottleneck_name": "Inadequate Commitment of Political and Technical Leadership",
                "bottleneck_description": (
                    "This bottleneck applies when there is a clear lack of sustained commitment by political or technical leaders to implement approved policies. "
                    "This includes delays, resistance, or failure to act when reforms threaten the status quo, require politically difficult trade-offs, "
                    "or demand resource shifts that are not followed through despite stated priorities. "
                    "Examples include: approved reforms not being enacted, persistent underfunding of a priority despite commitments, or misalignment between stated goals and actual budget execution. "
                    "Do **not** classify general governance weakness, vague statements, or budget/funding gaps **unless** directly tied to political/technical unwillingness or inaction. "
                    "Be careful to distinguish from other bottlenecks like 2.1 (coordination failures), 5.2 (disconnect between budgets and policy), or 6.3 (weak execution)."
                ),
                "model_key": "bottleneck_1_1"
            }
        ]
    },
    {
        "challenge_id": 2,
        "role_of_public_finance": "Commitment to Feasible Policy",
        "role_description": (
            "Finance ministries and other central agencies play a fundamental coordinating role when it comes to policy decisions, "
            "functioning as a clearing house for various policy proposals across sector institutions, promoting dialogue and consultation around policy trade-offs, "
            "and ultimately linking policy objectives with resource availability, mobilization and use. "
            "This helps build commitment to policy decisions that are made and improves their feasibility and the likelihood that they will be implemented."
        ),
        "challenge_name": "Incoherence and Fragmentation of Policy",
        "challenge_description": (
            "Examines whether policies across sectors and institutions are aligned and mutually supportive, or whether they reflect siloed decision-making. "
            "This includes failures to coordinate across ministries, levels of government, or between state and non-state actors. "
            "Fragmentation often stems from weak interagency coordination, unclear mandates, or donor-driven parallel systems, resulting in contradictory or overlapping policies and reduced policy effectiveness."
        ),
        "bottlenecks": [
            {
                "bottleneck_id": "2.1",
                "bottleneck_name": "Fragmented, Inconsistent and Uncoordinated Policies Across or Within Sectors",
                "bottleneck_description": (
                    "Different government policies, both sectoral and cross-sectoral, are designed without taking into account the constraints and/or complementarities that exist due to other government policies and commitments. "
                    "This can be due to bureaucratic silos, management incentives, or external donor support, and results in policies that are inconsistent and difficult to implement, or in unnecessary duplications and overlaps. "
                    "Examples include contradictory eligibility criteria across programs, overlapping subsidy schemes, or lack of coordination between infrastructure and service delivery plans. "
                    "Do **not** classify general complaints about policy quality or governance unless fragmentation or inconsistency is **explicitly** tied to siloed decision-making or lack of coordination. "
                    "Be careful to distinguish from 1.1 (leadership inaction), 5.2 (budget misalignment), or 6.3 (implementation issues)."
                ),
                "model_key": "bottleneck_2_1"
            }
        ]
    },
    {
        "challenge_id": 3,
        "role_of_public_finance": "Commitment to Feasible Policy",
        "role_description": (
            "Finance ministries and other central agencies play a fundamental coordinating role when it comes to policy decisions, "
            "functioning as a clearing house for various policy proposals across sector institutions, promoting dialogue and consultation around policy trade-offs, "
            "and ultimately linking policy objectives with resource availability, mobilization and use. "
            "This helps build commitment to policy decisions that are made and improves their feasibility and the likelihood that they will be implemented."
        ),
        "challenge_name": "Mismatch Between Policy Goals, Capability and Resources",
        "challenge_description": (
            "Assesses whether the scope and ambition of public policies are realistically matched with the institutional capabilities and fiscal resources available. "
            "This includes examining whether domestic revenue, administrative capacity, and institutional mandates are adequate to deliver on stated goals."
        ),
        "bottlenecks": [
            {
                "bottleneck_id": "3.1",
                "bottleneck_name": "Domestic Revenue Policies Generate Insufficient Resources to Achieve Policy Goals Given Fiscal Reality",
                "bottleneck_description": (
                    "In many countries, due to economic structure (e.g., large informal sectors), poor policy design and weak administration capabilities, governments collect only 10–15% of GDP or less in revenues. "
                    "This limits their fiscal space and constrains spending on priority policies aimed at achieving development outcomes. "
                    "This bottleneck captures domestic revenue policies that fail to generate sufficient resources to achieve policy goals. These failures limits their fiscal capacity and fiscal space, constraining spending on priority policies aimed at achieving development outcomes. Studies on the financing gap for SDG achievement have shown this clearly. "
                    "Do **not** classify general references to budget constraints or underfunding **unless** they are linked to the structural limitations of the revenue system. "
                    "Be careful to distinguish from 1.1 (leadership inaction), 5.2 (disconnect between budget and policy), or 6.1 (fragmented funding channels)."
                ),
                "model_key": "bottleneck_3_1"
            }
        ]
    },
    {
        "challenge_id": 6,
        "role_of_public_finance": "Effective Resource Mobilization & Distribution",
        "role_description": (
            "Governments need to raise and allocate and influence private financial resources in support of the pursuit of their policy objectives, "
            "ensuring both that sufficient resources are available when needed, and that these are allocated according to needs and cost-effectiveness criteria. "
            "How governments do this has important distributional impacts and can influence public and private behaviour towards achievement of objectives."
        ),
        "challenge_name": "Unreliable, delayed and fragmented funding for delivery",
        "challenge_description": (
            "Assesses how predictable, timely, and well-coordinated funding flows are, and whether fragmentation or delays impede delivery."
        ),
        "bottlenecks": [
            {
                "bottleneck_id": "6.1",
                "bottleneck_name": "Ad hoc, Political and Fragmented Funding Channels",
                "bottleneck_description": (
                    "Governments and public sector entities often rely on multiple, uncoordinated funding mechanisms—such as general funds, earmarked revenues, donor funding, and intergovernmental transfers. "
                    "These mechanisms often lack integration, leading to volatility, administrative duplication, and fragmented service delivery. "
                    "Discretionary or politically influenced allocation, parallel management systems (especially from donors), and poor coordination across agencies or levels of government compound inefficiencies. "
                    "Common issues include delays, incomplete disbursements, excessive reporting burdens, and poor alignment of funding with long-term plans or shared objectives."
                ),
                "model_key": "bottleneck_6_1"
            }
        ]
    }

]