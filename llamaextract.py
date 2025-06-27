import os
from dotenv import load_dotenv
from llama_cloud_services import LlamaExtract
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any, Union
from datetime import date, datetime
from enum import Enum


# Load environment variables from .env file
load_dotenv()
LLAMA_CLOUD_API_KEY = os.getenv("LLAMA_CLOUD")

extractor = LlamaExtract()

class SourceAuthority(str, Enum):
    FASB = "FASB"
    SEC = "SEC"
    AICPA = "AICPA"
    OTHER = "Other"


class RelationshipType(str, Enum):
    RELATED = "related"
    SUPERSEDES = "supersedes"
    AMENDED_BY = "amended_by"
    CLARIFIED_BY = "clarified_by"
    MODIFIED_BY = "modified_by"


class SectionType(str, Enum):
    OVERVIEW = "Overview"
    RECOGNITION = "Recognition"
    INITIAL_MEASUREMENT = "Initial Measurement"
    SUBSEQUENT_MEASUREMENT = "Subsequent Measurement"
    DERECOGNITION = "Derecognition"
    PRESENTATION = "Presentation"
    DISCLOSURE = "Disclosure"
    IMPLEMENTATION_GUIDANCE = "Implementation Guidance"
    GLOSSARY = "Glossary"
    TRANSITION = "Transition"
    EFFECTIVE_DATE = "Effective Date"


class ParagraphType(str, Enum):
    PRINCIPLE = "principle"
    REQUIREMENT = "requirement"
    GUIDANCE = "guidance"
    EXCEPTION = "exception"
    DEFINITION = "definition"
    EXAMPLE = "example"
    ILLUSTRATION = "illustration"


class MandatoryLanguage(str, Enum):
    SHALL = "shall"
    SHOULD = "should"
    MUST = "must"
    REQUIRED = "required"
    PROHIBITED = "prohibited"
    MAY = "may"
    MIGHT = "might"


class RequirementType(str, Enum):
    RECOGNITION = "recognition"
    MEASUREMENT = "measurement"
    DISCLOSURE = "disclosure"
    PRESENTATION = "presentation"
    TRANSITION = "transition"
    SCOPE = "scope"


class JournalEntry(BaseModel):
    account: str = Field(..., description="Account name")
    debit: Optional[float] = Field(None, description="Debit amount")
    credit: Optional[float] = Field(None, description="Credit amount")
    description: Optional[str] = Field(None, description="Entry description")

    @validator('debit', 'credit')
    def validate_amounts(cls, v):
        if v is not None and v < 0:
            raise ValueError('Debit and credit amounts must be non-negative')
        return v


class Requirement(BaseModel):
    requirement_text: str = Field(..., description="Specific requirement extracted from paragraph")
    mandatory_language: List[MandatoryLanguage] = Field(
        default_factory=list,
        description="Language indicating requirement level"
    )
    conditions: List[str] = Field(
        default_factory=list,
        description="Conditions under which requirement applies"
    )
    exceptions: List[str] = Field(
        default_factory=list,
        description="Exceptions to the requirement"
    )


class Example(BaseModel):
    example_id: str = Field(..., description="Example identifier")
    title: Optional[str] = Field(None, description="Example title")
    scenario: str = Field(..., description="Scenario description")
    analysis: str = Field(..., description="Analysis and conclusion")
    journal_entries: List[JournalEntry] = Field(
        default_factory=list,
        description="Related journal entries"
    )
    related_paragraphs: List[str] = Field(
        default_factory=list,
        description="References to related paragraph IDs"
    )
    financial_statement_impact: Optional[str] = Field(
        None,
        description="Impact on financial statements"
    )


class Paragraph(BaseModel):
    paragraph_id: str = Field(..., description="Unique paragraph identifier (e.g., '606-10-25-1')")
    text: str = Field(..., description="Paragraph content")
    paragraph_type: ParagraphType = Field(..., description="Type of content in the paragraph")
    key_terms: List[str] = Field(
        default_factory=list,
        description="Key accounting terms defined or used"
    )
    requirements: List[Requirement] = Field(
        default_factory=list,
        description="Specific requirements extracted from paragraph"
    )
    conditions: List[str] = Field(
        default_factory=list,
        description="Conditions under which the guidance applies"
    )
    cross_references: List[str] = Field(
        default_factory=list,
        description="References to other paragraphs or topics"
    )
    effective_date: Optional[date] = Field(
        None,
        description="Specific effective date if different from topic"
    )
    
    @validator('paragraph_id')
    def validate_paragraph_id(cls, v):
        # Basic validation for ASC paragraph ID format
        parts = v.split('-')
        if len(parts) < 3:
            raise ValueError('Paragraph ID must follow ASC format (e.g., 606-10-25-1)')
        return v


class SubSection(BaseModel):
    subsection_number: str = Field(..., description="Subsection number (e.g., '25-1', '25-2')")
    subsection_title: Optional[str] = Field(None, description="Title of the subsection")
    summary: Optional[str] = Field(None, description="Brief summary of subsection content")
    paragraphs: List[Paragraph] = Field(
        default_factory=list,
        description="Paragraphs within this subsection"
    )
    examples: List[Example] = Field(
        default_factory=list,
        description="Examples illustrating the guidance"
    )


class Section(BaseModel):
    section_number: str = Field(..., description="Section number (e.g., '10', '20', '25')")
    section_title: str = Field(..., description="Title of the section")
    section_type: SectionType = Field(..., description="Type of guidance provided in this section")
    summary: Optional[str] = Field(None, description="Brief summary of section content")
    subsections: List[SubSection] = Field(
        default_factory=list,
        description="Subsections within this section"
    )
    
    @validator('section_number')
    def validate_section_number(cls, v):
        if not v.isdigit() or len(v) != 2:
            raise ValueError('Section number must be a two-digit string')
        return v


class Overview(BaseModel):
    general: Optional[str] = Field(None, description="General overview of the topic")
    scope: Optional[str] = Field(None, description="Scope and applicability")
    key_changes: List[str] = Field(
        default_factory=list,
        description="Key changes from previous guidance"
    )
    objectives: List[str] = Field(
        default_factory=list,
        description="Primary objectives of the standard"
    )
    core_principles: List[str] = Field(
        default_factory=list,
        description="Core principles underlying the guidance"
    )


class CodificationStructure(BaseModel):
    overview: Optional[Overview] = Field(None, description="Topic overview")
    sections: List[Section] = Field(..., description="Main sections of the codification")


class CrossReference(BaseModel):
    referenced_topic: str = Field(..., description="ASC topic being referenced")
    relationship_type: RelationshipType = Field(..., description="Type of relationship")
    description: str = Field(..., description="Description of the relationship")
    specific_paragraphs: List[str] = Field(
        default_factory=list,
        description="Specific paragraphs that relate to this reference"
    )


class IndustryGuidance(BaseModel):
    industry: str = Field(..., description="Specific industry (e.g., 'Banking', 'Insurance', 'Software')")
    industry_code: Optional[str] = Field(None, description="Industry classification code")
    specific_guidance: str = Field(..., description="Industry-specific guidance or exceptions")
    affected_sections: List[str] = Field(
        default_factory=list,
        description="Sections affected by industry-specific guidance"
    )


class ComplianceCheckpoint(BaseModel):
    checkpoint_id: str = Field(..., description="Unique identifier for the compliance checkpoint")
    requirement_type: RequirementType = Field(..., description="Type of requirement")
    requirement_text: str = Field(..., description="The specific requirement text")
    assessment_criteria: List[str] = Field(
        default_factory=list,
        description="Criteria for assessing compliance"
    )
    common_violations: List[str] = Field(
        default_factory=list,
        description="Common ways this requirement is violated"
    )
    testing_procedures: List[str] = Field(
        default_factory=list,
        description="Procedures for testing compliance"
    )
    documentation_requirements: List[str] = Field(
        default_factory=list,
        description="Required documentation for compliance"
    )
    risk_level: Optional[str] = Field(
        None,
        description="Risk level (High, Medium, Low)"
    )
    related_paragraphs: List[str] = Field(
        default_factory=list,
        description="Paragraph IDs that support this checkpoint"
    )


class DocumentMetadata(BaseModel):
    asc_topic: str = Field(..., description="ASC topic number (e.g., '606', '842', '326')")
    topic_title: str = Field(..., description="Full title of the ASC topic")
    version: str = Field(..., description="Version or revision identifier")
    effective_date: date = Field(..., description="Effective date of the standard")
    last_updated: datetime = Field(..., description="Last update timestamp")
    source_authority: SourceAuthority = Field(..., description="Authoritative body that issued the standard")
    superseded_guidance: List[str] = Field(
        default_factory=list,
        description="Previously superseded standards or guidance"
    )
    related_topics: List[str] = Field(
        default_factory=list,
        description="Related ASC topics"
    )
    transition_guidance: Optional[str] = Field(
        None,
        description="Transition guidance from previous standards"
    )
    
    @validator('asc_topic')
    def validate_asc_topic(cls, v):
        if not v.isdigit() or len(v) != 3:
            raise ValueError('ASC topic must be a three-digit string')
        return v


class ASCFilings(BaseModel):
    """
    Comprehensive Pydantic model for ASC Codification documents
    optimized for RAG and compliance comparison with 10-K, 10-Q, and accounting policy manuals
    """
    
    document_metadata: DocumentMetadata = Field(..., description="Document metadata and identification")
    codification_structure: CodificationStructure = Field(..., description="Main codification content")
    cross_references: List[CrossReference] = Field(
        default_factory=list,
        description="Cross-references to other ASC topics"
    )
    industry_specific: List[IndustryGuidance] = Field(
        default_factory=list,
        description="Industry-specific guidance and exceptions"
    )
    compliance_checkpoints: List[ComplianceCheckpoint] = Field(
        default_factory=list,
        description="Compliance checkpoints for automated assessment"
    )
    
    # Additional fields for enhanced RAG capabilities
    glossary_terms: Dict[str, str] = Field(
        default_factory=dict,
        description="Glossary of terms defined in this topic"
    )
    
    implementation_guidance: Optional[str] = Field(
        None,
        description="General implementation guidance"
    )
    
    frequently_asked_questions: List[Dict[str, str]] = Field(
        default_factory=list,
        description="FAQ items with question and answer pairs"
    )
    
    # Metadata for RAG optimization
    semantic_tags: List[str] = Field(
        default_factory=list,
        description="Semantic tags for improved search and retrieval"
    )
    
    complexity_score: Optional[int] = Field(
        None,
        ge=1,
        le=10,
        description="Complexity score from 1-10 for the topic"
    )

agent = extractor.create_agent(name="ASC Codification Agent",
                               data_schema=ASCFilings)

result = agent.extrac