-- 1 --------------------------------------------------------------
CREATE TABLE employee_profiles (
    EmployeeID          INTEGER PRIMARY KEY,
    FirstName           TEXT,
    LastName            TEXT,
    MiddleInitial       TEXT,
    BirthDate           DATE,
    HireDate            DATE,
    TerminationDate     DATE,
    Gender              TEXT,
    MaritalStatus       TEXT,
    Nationality         TEXT,
    DepartmentID        INTEGER,
    JobTitle            TEXT,
    EmploymentType      TEXT,
    SalaryBase          REAL,
    SalaryBonus         REAL,
    Currency            TEXT,
    WorkLocation        TEXT,
    OfficePhone         TEXT,
    MobilePhone         TEXT,
    Email               TEXT,
    EmergencyContact    TEXT,
    EmergencyPhone      TEXT,
    IsActive            INTEGER,
    LastPromotionDate   DATE
);

-- 2 --------------------------------------------------------------
CREATE TABLE department_budget (
    DepartmentID        INTEGER PRIMARY KEY,
    DepartmentName      TEXT,
    FiscalYear          INTEGER,
    BudgetAllocated     REAL,
    BudgetUsed          REAL,
    Currency            TEXT,
    ManagerEmployeeID   INTEGER,
    CostCenterCode      TEXT,
    CapitalExpenditure  REAL,
    OperatingExpenditure REAL,
    HeadcountPlanned    INTEGER,
    HeadcountActual     INTEGER,
    LocationRegion      TEXT,
    ApprovalStatus      TEXT,
    LastReviewDate      DATE,
    NextReviewDate      DATE,
    FundingSource       TEXT,
    ProjectCode         TEXT,
    IsDeleted           INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 3 --------------------------------------------------------------
CREATE TABLE supplier_contracts (
    ContractID          INTEGER PRIMARY KEY,
    SupplierID          INTEGER,
    ContractNumber      TEXT,
    StartDate           DATE,
    EndDate             DATE,
    ContractValue       REAL,
    Currency            TEXT,
    PaymentTerms        TEXT,
    DeliveryTerms       TEXT,
    ServiceLevel        TEXT,
    RenewalOption       TEXT,
    PrimaryContactName  TEXT,
    PrimaryContactEmail TEXT,
    PrimaryContactPhone TEXT,
    Status              TEXT,
    GoverningLaw        TEXT,
    Audited             INTEGER,
    CreatedByUserID     INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsArchived          INTEGER
);

-- 4 --------------------------------------------------------------
CREATE TABLE product_inventory (
    InventoryID         INTEGER PRIMARY KEY,
    WarehouseID         INTEGER,
    SKU                 TEXT,
    BatchNumber         TEXT,
    QuantityOnHand      INTEGER,
    QuantityAllocated   INTEGER,
    QuantityDamaged     INTEGER,
    UnitCost            REAL,
    Currency            TEXT,
    LastRestockDate     DATE,
    NextRestockDate     DATE,
    ReorderPoint        INTEGER,
    SafetyStockLevel    INTEGER,
    ShelfLifeDays       INTEGER,
    TemperatureControlled INTEGER,
    LocationAisle       TEXT,
    LocationBin         TEXT,
    SupplierID          INTEGER,
    OwnerDepartmentID   INTEGER,
    IsActive            INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 5 --------------------------------------------------------------
CREATE TABLE shipment_tracking (
    TrackingID          INTEGER PRIMARY KEY,
    ShipmentID          INTEGER,
    CarrierCode         TEXT,
    ServiceLevel        TEXT,
    OriginPostalCode    TEXT,
    DestinationPostalCode TEXT,
    EstimatedDeparture  DATE,
    EstimatedArrival    DATE,
    ActualDeparture     DATE,
    ActualArrival       DATE,
    CurrentStatus       TEXT,
    WeightKg            REAL,
    VolumeCubicMeters   REAL,
    NumberOfPieces      INTEGER,
    FreightCost         REAL,
    Currency            TEXT,
    SignatureRequired   INTEGER,
    HazardousMaterial   INTEGER,
    LastUpdateTimestamp TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsDelivered         INTEGER
);

-- 6 --------------------------------------------------------------
CREATE TABLE maintenance_logs (
    LogID               INTEGER PRIMARY KEY,
    AssetID             INTEGER,
    AssetType           TEXT,
    MaintenanceDate     DATE,
    TechnicianID        INTEGER,
    Description         TEXT,
    PartsUsed           TEXT,
    LaborHours          REAL,
    LaborCost           REAL,
    PartsCost           REAL,
    TotalCost           REAL,
    Currency            TEXT,
    DowntimeMinutes     INTEGER,
    Preventive          INTEGER,
    FollowUpRequired    INTEGER,
    FollowUpDate        DATE,
    CreatedByUserID     INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsClosed            INTEGER,
    ClosureComment      TEXT
);

-- 7 --------------------------------------------------------------
CREATE TABLE marketing_campaigns (
    CampaignID          INTEGER PRIMARY KEY,
    CampaignName        TEXT,
    StartDate           DATE,
    EndDate             DATE,
    BudgetAllocated     REAL,
    Currency            TEXT,
    TargetAudience      TEXT,
    Channel             TEXT,
    CreativeAssetID     INTEGER,
    ImpressionsGoal     INTEGER,
    ClicksGoal          INTEGER,
    ConversionsGoal     INTEGER,
    CostPerImpression   REAL,
    CostPerClick        REAL,
    CostPerConversion   REAL,
    Status              TEXT,
    OwnerUserID         INTEGER,
    ApprovalStatus      TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsArchived          INTEGER,
    Notes               TEXT
);

-- 8 --------------------------------------------------------------
CREATE TABLE website_analytics (
    RecordID            INTEGER PRIMARY KEY,
    SiteDomain          TEXT,
    PageURL             TEXT,
    VisitDate           DATE,
    VisitorID           TEXT,
    SessionID           TEXT,
    DeviceType          TEXT,
    Browser             TEXT,
    OperatingSystem     TEXT,
    CountryCode         TEXT,
    City                TEXT,
    ReferralSource      TEXT,
    BounceRate          REAL,
    TimeOnPageSeconds   REAL,
    ScrollDepthPercent  REAL,
    Clicks              INTEGER,
    Conversions         INTEGER,
    RevenueGenerated    REAL,
    Currency            TEXT,
    IsNewVisitor        INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 9 --------------------------------------------------------------
CREATE TABLE iot_sensor_readings (
    ReadingID           INTEGER PRIMARY KEY,
    DeviceID            TEXT,
    SensorType          TEXT,
    ReadingTimestamp    TEXT,
    ValueNumeric        REAL,
    ValueText           TEXT,
    Unit                TEXT,
    BatteryLevelPercent INTEGER,
    SignalStrengthDbm   REAL,
    FirmwareVersion     TEXT,
    Latitude            REAL,
    Longitude           REAL,
    AltitudeMeters      REAL,
    StatusFlag          INTEGER,
    AlertGenerated      INTEGER,
    AlertType           TEXT,
    MaintenanceDueDate  DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    Comment             TEXT
);

-- 10 -------------------------------------------------------------
CREATE TABLE procurement_requests (
    RequestID           INTEGER PRIMARY KEY,
    RequestNumber       TEXT,
    RequestedByUserID   INTEGER,
    DepartmentID        INTEGER,
    RequestDate         DATE,
    RequiredByDate      DATE,
    Status              TEXT,
    PriorityLevel       TEXT,
    TotalEstimatedCost  REAL,
    Currency            TEXT,
    VendorPreference    TEXT,
    Justification       TEXT,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    Comments            TEXT,
    AttachmentsPath     TEXT,
    IsCancelled         INTEGER,
    CancelledByUserID   INTEGER,
    CancelledDate       DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 11 -------------------------------------------------------------
CREATE TABLE training_courses (
    CourseID            INTEGER PRIMARY KEY,
    CourseCode          TEXT,
    CourseName          TEXT,
    Description         TEXT,
    DepartmentID        INTEGER,
    TrainerEmployeeID   INTEGER,
    DurationHours       REAL,
    DeliveryMethod      TEXT,
    CostPerParticipant  REAL,
    Currency            TEXT,
    MaxParticipants     INTEGER,
    PrerequisiteCourseID INTEGER,
    CertificationEarned TEXT,
    StartDate           DATE,
    EndDate             DATE,
    EnrollmentOpen      INTEGER,
    EnrollmentCloseDate DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsRetired           INTEGER,
    Notes               TEXT
);

-- 12 -------------------------------------------------------------
CREATE TABLE asset_locations (
    LocationID          INTEGER PRIMARY KEY,
    AssetID             INTEGER,
    AssetType           TEXT,
    Building            TEXT,
    Floor               TEXT,
    RoomNumber          TEXT,
    ShelfNumber         TEXT,
    AssignedDate        DATE,
    UnassignedDate      DATE,
    CurrentCustodianID  INTEGER,
    ConditionRating     INTEGER,
    LastInspectionDate  DATE,
    NextInspectionDue   DATE,
    SecurityLevel       TEXT,
    AccessControlCode   TEXT,
    Latitude            REAL,
    Longitude           REAL,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    Comments            TEXT
);

-- 13 -------------------------------------------------------------
CREATE TABLE financial_transactions (
    TransactionID       INTEGER PRIMARY KEY,
    AccountNumber       TEXT,
    TransactionDate     DATE,
    PostingDate         DATE,
    Amount              REAL,
    Currency            TEXT,
    TransactionType     TEXT,
    Description         TEXT,
    CategoryID          INTEGER,
    SubCategoryID       INTEGER,
    Status              TEXT,
    ReferenceNumber     TEXT,
    BatchNumber         TEXT,
    ExchangeRate        REAL,
    FeeAmount           REAL,
    FeeCurrency         TEXT,
    IsReconciled        INTEGER,
    ReconciliationDate  DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsDeleted           INTEGER,
    InternalNote        TEXT
);

-- 14 -------------------------------------------------------------
CREATE TABLE legal_cases (
    CaseID              INTEGER PRIMARY KEY,
    CaseNumber          TEXT,
    PlaintiffName       TEXT,
    DefendantName       TEXT,
    FilingDate          DATE,
    Court               TEXT,
    Judge               TEXT,
    CaseStatus          TEXT,
    ClaimAmount         REAL,
    Currency            TEXT,
    SettlementAmount    REAL,
    SettlementDate      DATE,
    AttorneyID          INTEGER,
    LeadCounselID       INTEGER,
    Jurisdiction        TEXT,
    Confidential        INTEGER,
    DocumentPath        TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsClosed            INTEGER,
    CloseReason         TEXT
);

-- 15 -------------------------------------------------------------
CREATE TABLE research_projects (
    ProjectID           INTEGER PRIMARY KEY,
    ProjectCode         TEXT,
    Title               TEXT,
    LeadResearcherID    INTEGER,
    DepartmentID        INTEGER,
    StartDate           DATE,
    EndDate             DATE,
    FundingAgency       TEXT,
    FundingAmount       REAL,
    Currency            TEXT,
    GrantNumber         TEXT,
    Status              TEXT,
    PublicationCount    INTEGER,
    PatentCount         INTEGER,
    DataRepositoryURL   TEXT,
    EthicalApproval     INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsArchived          INTEGER,
    Notes               TEXT
);

-- 16 -------------------------------------------------------------
CREATE TABLE ticketing_system (
    TicketID            INTEGER PRIMARY KEY,
    TicketNumber        TEXT,
    SubmittedByUserID   INTEGER,
    AssignedToUserID    INTEGER,
    DepartmentID        INTEGER,
    Priority            TEXT,
    IssueCategory       TEXT,
    IssueSubCategory    TEXT,
    Description         TEXT,
    Status              TEXT,
    CreatedDate         DATE,
    UpdatedDate         DATE,
    ResolvedDate        DATE,
    ResolutionSummary   TEXT,
    SLAHours            REAL,
    Escalated           INTEGER,
    ClosureCode         TEXT,
    IsDuplicate         INTEGER,
    DuplicateOfTicketID INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 17 -------------------------------------------------------------
CREATE TABLE conference_events (
    EventID             INTEGER PRIMARY KEY,
    EventCode           TEXT,
    Title               TEXT,
    Description         TEXT,
    StartDateTime       TEXT,
    EndDateTime         TEXT,
    Venue               TEXT,
    City                TEXT,
    Country             TEXT,
    OrganizerUserID     INTEGER,
    Capacity            INTEGER,
    TicketsSold         INTEGER,
    TicketPrice         REAL,
    Currency            TEXT,
    SponsorshipLevel    TEXT,
    SponsorCompanyID    INTEGER,
    IsVirtual           INTEGER,
    LiveStreamURL       TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsCancelled         INTEGER,
    CancellationReason  TEXT
);

-- 18 -------------------------------------------------------------
CREATE TABLE loyalty_programs (
    ProgramID           INTEGER PRIMARY KEY,
    ProgramName         TEXT,
    Description         TEXT,
    LaunchDate          DATE,
    ExpirationDate      DATE,
    PointsEarnRate      REAL,
    PointsRedeemRate    REAL,
    Currency            TEXT,
    TierCount           INTEGER,
    TierNames           TEXT,
    EligibilityCriteria TEXT,
    EnrollmentMethod    TEXT,
    IsActive            INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    AdminUserID         INTEGER,
    TermsAndConditions  TEXT,
    PrivacyPolicyURL    TEXT,
    SupportContactEmail TEXT,
    SupportContactPhone TEXT,
    IsDeprecated        INTEGER
);

-- 19 -------------------------------------------------------------
CREATE TABLE data_governance_policies (
    PolicyID            INTEGER PRIMARY KEY,
    PolicyName          TEXT,
    Description         TEXT,
    EffectiveDate       DATE,
    ReviewDate          DATE,
    OwnerDepartmentID   INTEGER,
    ClassificationLevel TEXT,
    RetentionPeriodDays INTEGER,
    EncryptionRequired  INTEGER,
    AccessControlModel  TEXT,
    IsMandatory         INTEGER,
    VersionNumber       TEXT,
    Status              TEXT,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    RevisionNotes       TEXT,
    DocumentationURL    TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsObsolete          INTEGER,
    ObsoletionDate      DATE
);

-- 20 -------------------------------------------------------------
CREATE TABLE software_licenses (
    LicenseID           INTEGER PRIMARY KEY,
    SoftwareName        TEXT,
    LicenseKey          TEXT,
    Vendor              TEXT,
    PurchaseDate        DATE,
    ExpirationDate      DATE,
    SeatsPurchased      INTEGER,
    SeatsInUse          INTEGER,
    LicenseType         TEXT,
    Cost                REAL,
    Currency            TEXT,
    RenewalReminderDays INTEGER,
    AssignedToUserID    INTEGER,
    DepartmentID        INTEGER,
    IsComplianceChecked INTEGER,
    ComplianceNotes     TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    DecommissionDate    DATE,
    Comments            TEXT
);

-- 21 -------------------------------------------------------------
CREATE TABLE quality_inspections (
    InspectionID        INTEGER PRIMARY KEY,
    InspectionDate      DATE,
    InspectorID         INTEGER,
    FacilityID          INTEGER,
    ProductionLineID    INTEGER,
    ProductSKU          TEXT,
    DefectCount         INTEGER,
    DefectSeverity      TEXT,
    CorrectiveAction    TEXT,
    ReworkTimeMinutes   INTEGER,
    InspectionResult   TEXT,
    PassFail            TEXT,
    SampleSize          INTEGER,
    SamplingMethod      TEXT,
    IsCritical          INTEGER,
    Comments            TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsClosed            INTEGER,
    ClosureDate         DATE,
    FollowUpInspectionID INTEGER
);

-- 22 -------------------------------------------------------------
CREATE TABLE disaster_recovery_plans (
    PlanID              INTEGER PRIMARY KEY,
    PlanName            TEXT,
    Description         TEXT,
    OwnerDepartmentID   INTEGER,
    CriticalSystems     TEXT,
    RTOHours            REAL,
    RPOHours            REAL,
    BackupLocation      TEXT,
    BackupFrequency     TEXT,
    TestingFrequency    TEXT,
    LastTestDate        DATE,
    TestResult          TEXT,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    Status              TEXT,
    VersionNumber       TEXT,
    DocumentationURL    TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsRetired           INTEGER,
    RetirementDate      DATE
);

-- 23 -------------------------------------------------------------
CREATE TABLE fleet_vehicles (
    VehicleID           INTEGER PRIMARY KEY,
    VIN                 TEXT,
    LicensePlate        TEXT,
    Make                TEXT,
    Model               TEXT,
    Year                INTEGER,
    OdometerKm          INTEGER,
    FuelType            TEXT,
    EngineSizeL        REAL,
    AssignedDriverID    INTEGER,
    DepartmentID        INTEGER,
    PurchaseDate        DATE,
    LeaseEndDate        DATE,
    InsurancePolicyNum  TEXT,
    InsuranceExpiryDate DATE,
    MaintenanceSchedule TEXT,
    LastServiceDate     DATE,
    ServiceDueKm        INTEGER,
    IsActive            INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 24 -------------------------------------------------------------
CREATE TABLE building_facilities (
    FacilityID          INTEGER PRIMARY KEY,
    FacilityName        TEXT,
    AddressLine1        TEXT,
    AddressLine2        TEXT,
    City                TEXT,
    StateProvince       TEXT,
    PostalCode          TEXT,
    Country             TEXT,
    BuildingType        TEXT,
    GrossFloorAreaSqM   REAL,
    YearConstructed     INTEGER,
    OwnerDepartmentID   INTEGER,
    FacilityManagerID   INTEGER,
    HVACSystemType      TEXT,
    SecuritySystemType  TEXT,
    FireSafetyRating    TEXT,
    EnergyStarRating    TEXT,
    OccupancyStatus     TEXT,
    LastRenovationDate  DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsOperational       INTEGER,
    DecommissionDate    DATE
);

-- 25 -------------------------------------------------------------
CREATE TABLE medical_claims (
    ClaimID             INTEGER PRIMARY KEY,
    PatientID           INTEGER,
    ProviderID          INTEGER,
    ClaimDate           DATE,
    ServiceDate         DATE,
    DiagnosisCode       TEXT,
    ProcedureCode       TEXT,
    BilledAmount        REAL,
    ApprovedAmount      REAL,
    Currency            TEXT,
    CopayAmount         REAL,
    DeductibleAmount    REAL,
    ClaimStatus         TEXT,
    Payer               TEXT,
    AuthorizationNumber TEXT,
    Notes               TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsReversed          INTEGER,
    ReversalDate        DATE,
    ReversalReason      TEXT
);

-- 26 -------------------------------------------------------------
CREATE TABLE e_commerce_orders (
    OrderID             INTEGER PRIMARY KEY,
    OrderNumber         TEXT,
    CustomerID          INTEGER,
    OrderDate           DATE,
    ShipDate            DATE,
    DeliveryDate        DATE,
    ShippingMethod      TEXT,
    ShippingCost        REAL,
    Currency            TEXT,
    OrderTotal          REAL,
    TaxAmount           REAL,
    DiscountAmount      REAL,
    PaymentMethod       TEXT,
    PaymentStatus       TEXT,
    FulfillmentStatus   TEXT,
    BillingAddressID    INTEGER,
    ShippingAddressID   INTEGER,
    CouponCode          TEXT,
    IsGift              INTEGER,
    GiftMessage         TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsCancelled         INTEGER,
    CancellationReason  TEXT
);

-- 27 -------------------------------------------------------------
CREATE TABLE ad_campaign_performance (
    CampaignID          INTEGER,
    Date                DATE,
    Impressions         INTEGER,
    Clicks              INTEGER,
    Conversions         INTEGER,
    Spend               REAL,
    Revenue             REAL,
    Currency            TEXT,
    CostPerClick        REAL,
    CostPerConversion   REAL,
    ReturnOnAdSpend     REAL,
    ClickThroughRate    REAL,
    ConversionRate      REAL,
    DeviceCategory      TEXT,
    GeographicRegion    TEXT,
    PrimaryKey (CampaignID, Date)
);

-- 28 -------------------------------------------------------------
CREATE TABLE customer_feedback (
    FeedbackID          INTEGER PRIMARY KEY,
    CustomerID          INTEGER,
    FeedbackDate        DATE,
    Channel             TEXT,
    RatingOverall       INTEGER,
    RatingService       INTEGER,
    RatingProduct       INTEGER,
    Comments            TEXT,
    FollowUpRequired    INTEGER,
    FollowUpByUserID    INTEGER,
    FollowUpDate        DATE,
    ResolutionStatus    TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsEscalated         INTEGER,
    EscalationReason    TEXT,
    SurveyVersion       TEXT,
    NPSScore            INTEGER,
    SentimentScore      REAL,
    IsAnonymous         INTEGER
);

-- 29 -------------------------------------------------------------
CREATE TABLE payroll_runs (
    PayrollRunID        INTEGER PRIMARY KEY,
    RunDate             DATE,
    PayPeriodStart      DATE,
    PayPeriodEnd        DATE,
    Currency            TEXT,
    TotalGrossPay       REAL,
    TotalNetPay         REAL,
    TotalTaxWithheld    REAL,
    TotalBenefitsCost   REAL,
    TotalDeductions     REAL,
    NumberOfEmployees   INTEGER,
    ProcessedByUserID   INTEGER,
    Status              TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsFinalized         INTEGER,
    FinalizationDate    DATE,
    Comments            TEXT,
    PayFrequency        TEXT,
    PayrollProviderID   INTEGER
);

-- 30 -------------------------------------------------------------
CREATE TABLE risk_assessments (
    AssessmentID        INTEGER PRIMARY KEY,
    AssessmentDate      DATE,
    AssessorUserID      INTEGER,
    BusinessUnitID      INTEGER,
    RiskCategory        TEXT,
    RiskDescription     TEXT,
    LikelihoodScore     INTEGER,
    ImpactScore         INTEGER,
    RiskRating          TEXT,
    MitigationPlan      TEXT,
    OwnerUserID         INTEGER,
    DueDate             DATE,
    Status              TEXT,
    ReviewDate          DATE,
    ReviewedByUserID    INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsClosed            INTEGER,
    ClosureReason       TEXT,
    DocumentationURL    TEXT,
    ResidualRiskScore   INTEGER
);

-- 31 -------------------------------------------------------------
CREATE TABLE data_center_assets (
    AssetID             INTEGER PRIMARY KEY,
    AssetTag            TEXT,
    AssetType           TEXT,
    Manufacturer        TEXT,
    Model               TEXT,
    SerialNumber        TEXT,
    PurchaseDate        DATE,
    WarrantyEndDate     DATE,
    LocationRack        TEXT,
    LocationUPosition   TEXT,
    PowerConsumptionW   REAL,
    NetworkPortCount    INTEGER,
    FirmwareVersion     TEXT,
    IsVirtual           INTEGER,
    VirtualHostID       INTEGER,
    OperatingSystem     TEXT,
    OSVersion           TEXT,
    LastPatchDate       DATE,
    IsDecommissioned    INTEGER,
    DecommissionDate    DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 32 -------------------------------------------------------------
CREATE TABLE procurement_orders (
    POID                INTEGER PRIMARY KEY,
    PONumber            TEXT,
    SupplierID          INTEGER,
    OrderDate           DATE,
    RequiredDate        DATE,
    Currency            TEXT,
    TotalAmount         REAL,
    TaxAmount           REAL,
    ShippingCost        REAL,
    DiscountAmount      REAL,
    Status              TEXT,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    ReceivingDepartmentID INTEGER,
    FreightMethod       TEXT,
    FreightTerms        TEXT,
    IsDropShip          INTEGER,
    DropShipLocationID  INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsCancelled         INTEGER,
    CancelReason        TEXT,
    Comments            TEXT
);

-- 33 -------------------------------------------------------------
CREATE TABLE warranty_claims (
    ClaimID             INTEGER PRIMARY KEY,
    ProductSerialNumber TEXT,
    CustomerID          INTEGER,
    PurchaseDate        DATE,
    ClaimDate           DATE,
    IssueDescription    TEXT,
    RemedyRequested     TEXT,
    ClaimStatus         TEXT,
    ApprovedAmount      REAL,
    Currency            TEXT,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    ResolutionDate      DATE,
    ResolutionNotes     TEXT,
    IsRefund            INTEGER,
    IsReplacement       INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsEscalated         INTEGER,
    EscalationReason    TEXT
);

-- 34 -------------------------------------------------------------
CREATE TABLE subscription_plans (
    PlanID              INTEGER PRIMARY KEY,
    PlanName            TEXT,
    Description         TEXT,
    BillingCycle        TEXT,
    Price               REAL,
    Currency            TEXT,
    TrialPeriodDays    INTEGER,
    MaxUsers            INTEGER,
    IncludedFeatures    TEXT,
    IsActive            INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsDeprecated        INTEGER,
    DeprecationDate     DATE,
    SupportLevel        TEXT,
    SLAResponseTime     TEXT,
    CancelPenalty       REAL,
    PenaltyCurrency      TEXT,
    PromoCodeAllowed   INTEGER,
    TermsURL            TEXT
);

-- 35 -------------------------------------------------------------
CREATE TABLE credit_scores (
    ScoreID             INTEGER PRIMARY KEY,
    PersonID            INTEGER,
    ScoreDate           DATE,
    ScoreValue          INTEGER,
    ScoreProvider       TEXT,
    RiskCategory        TEXT,
    IsHardInquiry       INTEGER,
    InquiryDate         DATE,
    InquirySource       TEXT,
    Comments            TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsFlagged           INTEGER,
    FlagReason          TEXT,
    ExportedToFile      INTEGER,
    ExportDate          DATE,
    ReviewUserID        INTEGER,
    ReviewDate          DATE,
    AdjustedScore       INTEGER,
    AdjustmentReason    TEXT
);

-- 36 -------------------------------------------------------------
CREATE TABLE employee_benefits (
    BenefitID           INTEGER PRIMARY KEY,
    EmployeeID          INTEGER,
    BenefitType         TEXT,
    Provider            TEXT,
    PlanName            TEXT,
    CoverageStartDate   DATE,
    CoverageEndDate     DATE,
    EmployeeContribution REAL,
    EmployerContribution REAL,
    Currency            TEXT,
    EnrollmentStatus    TEXT,
    DependentCount      INTEGER,
    BenefitStatus       TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    TerminationDate     DATE,
    TerminationReason   TEXT,
    BenefitCardNumber   TEXT,
    CardExpirationDate  DATE
);

-- 37 -------------------------------------------------------------
CREATE TABLE server_logs (
    LogID               INTEGER PRIMARY KEY,
    ServerID            INTEGER,
    Timestamp           TEXT,
    LogLevel            TEXT,
    MessageText         TEXT,
    ProcessID           INTEGER,
    ThreadID            INTEGER,
    UserName            TEXT,
    SessionID           TEXT,
    SourceIP            TEXT,
    DestinationIP       TEXT,
    Port                INTEGER,
    HttpMethod          TEXT,
    UrlPath             TEXT,
    StatusCode          INTEGER,
    BytesSent           INTEGER,
    BytesReceived       INTEGER,
    CorrelationId       TEXT,
    StackTrace          TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 38 -------------------------------------------------------------
CREATE TABLE product_reviews (
    ReviewID            INTEGER PRIMARY KEY,
    ProductID           INTEGER,
    CustomerID          INTEGER,
    ReviewDate          DATE,
    RatingOverall       INTEGER,
    RatingQuality       INTEGER,
    RatingValueForMoney INTEGER,
    ReviewTitle         TEXT,
    ReviewBody          TEXT,
    VerifiedPurchase    INTEGER,
    HelpfulVotes        INTEGER,
    NotHelpfulVotes     INTEGER,
    ResponseByVendor    TEXT,
    ResponseDate        DATE,
    IsFlagged           INTEGER,
    FlagReason          TEXT,
    SentimentScore      REAL,
    Platform            TEXT,
    SourceURL           TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 39 -------------------------------------------------------------
CREATE TABLE equipment_maintenance_schedules (
    ScheduleID          INTEGER PRIMARY KEY,
    EquipmentID         INTEGER,
    MaintenanceType     TEXT,
    FrequencyDays       INTEGER,
    NextDueDate         DATE,
    LastPerformedDate   DATE,
    TechnicianID        INTEGER,
    EstimatedDurationMin INTEGER,
    RequiredParts       TEXT,
    SafetyProcedures    TEXT,
    IsCritical          INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    DeactivatedDate     DATE,
    Remarks             TEXT,
    NotificationSent    INTEGER,
    NotificationDate    DATE,
    EscalationLevel     TEXT,
    EscalationContactID INTEGER
);

-- 40 -------------------------------------------------------------
CREATE TABLE alumni_network (
    AlumniID            INTEGER PRIMARY KEY,
    FirstName           TEXT,
    LastName            TEXT,
    GraduationYear      INTEGER,
    Degree              TEXT,
    Major               TEXT,
    CurrentEmployer     TEXT,
    CurrentTitle        TEXT,
    Email               TEXT,
    Phone               TEXT,
    City                TEXT,
    StateProvince       TEXT,
    Country             TEXT,
    LinkedInURL         TEXT,
    IsDonor             INTEGER,
    TotalDonations      REAL,
    Currency            TEXT,
    LastDonationDate    DATE,
    MembershipStatus    TEXT,
    JoinedDate          DATE,
    IsActive            INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 41 -------------------------------------------------------------
CREATE TABLE patent_applications (
    PatentID            INTEGER PRIMARY KEY,
    ApplicationNumber   TEXT,
    Title               TEXT,
    InventorIDs         TEXT,
    FilingDate          DATE,
    PublicationDate     DATE,
    GrantDate           DATE,
    Status              TEXT,
    TechnologyArea      TEXT,
    Abstract            TEXT,
    ClaimsCount         INTEGER,
    PriorityCountry     TEXT,
    PriorityDate        DATE,
    AssociatedProjectID INTEGER,
    LegalCounselID      INTEGER,
    FeesPaid            REAL,
    Currency            TEXT,
    IsFamilyMember      INTEGER,
    FamilyPatentID      INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 42 -------------------------------------------------------------
CREATE TABLE employee_time_entries (
    TimeEntryID         INTEGER PRIMARY KEY,
    EmployeeID          INTEGER,
    ProjectID           INTEGER,
    Date                DATE,
    StartTime           TEXT,
    EndTime             TEXT,
    HoursWorked         REAL,
    Billable            INTEGER,
    Description         TEXT,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    IsOvertime          INTEGER,
    OvertimeRate        REAL,
    CostCenterCode      TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsDeleted           INTEGER,
    DeletionDate        DATE,
    DeletionReason      TEXT,
    TimesheetPeriodID   INTEGER
);

-- 43 -------------------------------------------------------------
CREATE TABLE corporate_events (
    EventID             INTEGER PRIMARY KEY,
    EventName           TEXT,
    Description         TEXT,
    EventDate           DATE,
    StartTime           TEXT,
    EndTime             TEXT,
    Venue               TEXT,
    City                TEXT,
    Country             TEXT,
    OrganizerUserID     INTEGER,
    ExpectedAttendees   INTEGER,
    ActualAttendees     INTEGER,
    BudgetAllocated     REAL,
    Currency            TEXT,
    SponsorshipLevel    TEXT,
    SponsorCompanyID    INTEGER,
    IsVirtual           INTEGER,
    LiveStreamURL       TEXT,
    RegistrationLink    TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsCancelled         INTEGER,
    CancellationReason  TEXT
);

-- 44 -------------------------------------------------------------
CREATE TABLE safety_incidents (
    IncidentID          INTEGER PRIMARY KEY,
    IncidentDate        DATE,
    ReportedByUserID    INTEGER,
    LocationID          INTEGER,
    Description         TEXT,
    SeverityLevel       TEXT,
    InjuryCount         INTEGER,
    LostTimeDays        INTEGER,
    RootCause           TEXT,
    CorrectiveAction    TEXT,
    FollowUpDate        DATE,
    FollowUpOwnerID     INTEGER,
    Status              TEXT,
    InvestigationReport TEXT,
    IsRegulated        INTEGER,
    RegulatoryBody      TEXT,
    PenaltyAmount       REAL,
    Currency            TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsClosed            INTEGER,
    ClosureDate         DATE,
    ClosureNotes        TEXT
);

-- 45 -------------------------------------------------------------
CREATE TABLE digital_assets (
    AssetID             INTEGER PRIMARY KEY,
    AssetName           TEXT,
    AssetType           TEXT,
    FilePath            TEXT,
    FileSizeBytes       INTEGER,
    MediaType           TEXT,
    WidthPixels         INTEGER,
    HeightPixels        INTEGER,
    DurationSeconds     REAL,
    OwnerUserID         INTEGER,
    CreatedDate         DATE,
    ModifiedDate        DATE,
    Tags                TEXT,
    LicenseType         TEXT,
    CopyrightHolder     TEXT,
    AccessLevel         TEXT,
    IsArchived          INTEGER,
    ArchiveDate         DATE,
    Description         TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 46 -------------------------------------------------------------
CREATE TABLE vendor_scorecards (
    ScorecardID         INTEGER PRIMARY KEY,
    VendorID            INTEGER,
    EvaluationPeriod    TEXT,
    QualityScore        REAL,
    DeliveryScore       REAL,
    CostScore           REAL,
    ComplianceScore     REAL,
    InnovationScore     REAL,
    OverallScore        REAL,
    ReviewerUserID      INTEGER,
    ReviewDate          DATE,
    Comments            TEXT,
    IsApproved          INTEGER,
    ApprovalDate        DATE,
    ApprovedByUserID    INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT
);

-- 47 -------------------------------------------------------------
CREATE TABLE facility_maintenance_requests (
    RequestID           INTEGER PRIMARY KEY,
    FacilityID          INTEGER,
    RequestDate         DATE,
    RequestorUserID     INTEGER,
    IssueCategory       TEXT,
    IssueDescription    TEXT,
    PriorityLevel       TEXT,
    AssignedToUserID    INTEGER,
    ScheduledDate       DATE,
    CompletionDate      DATE,
    Status              TEXT,
    ResolutionNotes     TEXT,
    CostEstimate        REAL,
    Currency            TEXT,
    IsEmergency         INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsClosed            INTEGER,
    ClosureReason       TEXT,
    FollowUpRequired    INTEGER,
    FollowUpDate        DATE
);

-- 48 -------------------------------------------------------------
CREATE TABLE customer_loyalty_redemptions (
    RedemptionID        INTEGER PRIMARY KEY,
    LoyaltyProgramID   INTEGER,
    CustomerID          INTEGER,
    RedemptionDate      DATE,
    PointsRedeemed      INTEGER,
    RewardType          TEXT,
    RewardDescription   TEXT,
    RedemptionStatus    TEXT,
    ProcessedByUserID   INTEGER,
    ProcessedDate       DATE,
    ExpirationDate      DATE,
    IsPartial           INTEGER,
    PartialPointsLeft   INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsCancelled         INTEGER,
    CancellationReason  TEXT,
    Notes               TEXT,
    RedemptionChannel   TEXT
);

-- 49 -------------------------------------------------------------
CREATE TABLE supplier_performance (
    SupplierID          INTEGER,
    EvaluationPeriod    TEXT,
    OnTimeDeliveryPct  REAL,
    DefectRatePct       REAL,
    CostVariancePct     REAL,
    ResponsivenessScore REAL,
    CollaborationScore  REAL,
    OverallScore        REAL,
    PrimaryKey (SupplierID, EvaluationPeriod)
);

-- 50 -------------------------------------------------------------
CREATE TABLE data_quality_rules (
    RuleID              INTEGER PRIMARY KEY,
    RuleName            TEXT,
    Description         TEXT,
    TargetTable         TEXT,
    TargetColumn        TEXT,
    RuleType            TEXT,
    Expression          TEXT,
    SeverityLevel       TEXT,
    OwnerUserID         INTEGER,
    CreatedDate         DATE,
    ModifiedDate        DATE,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    LastRunDate         DATE,
    LastRunStatus       TEXT,
    NotificationChannel TEXT,
    NotificationRecipient TEXT,
    DocumentationURL    TEXT,
    Tags                TEXT
);

-- 51 -------------------------------------------------------------
CREATE TABLE forecast_models (
    ModelID             INTEGER PRIMARY KEY,
    ModelName           TEXT,
    Description         TEXT,
    TargetTable         TEXT,
    TargetColumn        TEXT,
    Algorithm           TEXT,
    TrainingStartDate   DATE,
    TrainingEndDate     DATE,
    AccuracyMetric      TEXT,
    AccuracyValue       REAL,
    OwnerUserID         INTEGER,
    VersionNumber       TEXT,
    IsProduction        INTEGER,
    DeploymentDate      DATE,
    LastRetrainDate     DATE,
    RetrainFrequencyDays INTEGER,
    Dependencies        TEXT,
    DocumentationURL    TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 52 -------------------------------------------------------------
CREATE TABLE email_campaigns (
    CampaignID          INTEGER PRIMARY KEY,
    CampaignName        TEXT,
    SubjectLine         TEXT,
    FromAddress         TEXT,
    SentDate            DATE,
    TotalRecipients     INTEGER,
    OpenRate            REAL,
    ClickThroughRate    REAL,
    BounceRate          REAL,
    UnsubscribeRate     REAL,
    SpamComplaintRate   REAL,
    RevenueGenerated    REAL,
    Currency            TEXT,
    A/BTestGroup        TEXT,
    VariantID           INTEGER,
    OwnerUserID         INTEGER,
    Status              TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsScheduled         INTEGER,
    ScheduledSendTime   TEXT
);

-- 53 -------------------------------------------------------------
CREATE TABLE product_pricing_history (
    PriceHistoryID      INTEGER PRIMARY KEY,
    ProductID           INTEGER,
    EffectiveDate       DATE,
    EndDate             DATE,
    ListPrice           REAL,
    DiscountPrice       REAL,
    Currency            TEXT,
    PriceTier           TEXT,
    RegionCode          TEXT,
    CreatedByUserID     INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    ReasonForChange     TEXT,
    PromotionCode       TEXT
);

-- 54 -------------------------------------------------------------
CREATE TABLE insurance_policies (
    PolicyID            INTEGER PRIMARY KEY,
    PolicyNumber        TEXT,
    InsuredEntityID     INTEGER,
    ProviderID          INTEGER,
    CoverageType        TEXT,
    CoverageStartDate   DATE,
    CoverageEndDate     DATE,
    PremiumAmount       REAL,
    Currency            TEXT,
    DeductibleAmount   REAL,
    IsActive            INTEGER,
    LastRenewalDate     DATE,
    NextRenewalDate     DATE,
    BrokerUserID        INTEGER,
    ClaimHistoryLink    TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsCancelled         INTEGER,
    CancellationReason  TEXT,
    Notes               TEXT
);

-- 55 -------------------------------------------------------------
CREATE TABLE manufacturing_orders (
    OrderID             INTEGER PRIMARY KEY,
    OrderNumber         TEXT,
    ProductID           INTEGER,
    QuantityPlanned     INTEGER,
    QuantityProduced    INTEGER,
    StartDate           DATE,
    PlannedCompletionDate DATE,
    ActualCompletionDate DATE,
    ProductionLineID    INTEGER,
    Shift               TEXT,
    SupervisorUserID    INTEGER,
    Status              TEXT,
    ScrapQuantity       INTEGER,
    ReworkQuantity      INTEGER,
    DowntimeMinutes     INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsClosed            INTEGER,
    ClosureReason       TEXT,
    Notes               TEXT
);

-- 56 -------------------------------------------------------------
CREATE TABLE warehouse_zones (
    ZoneID              INTEGER PRIMARY KEY,
    WarehouseID         INTEGER,
    ZoneName            TEXT,
    Description         TEXT,
    CapacitySqM         REAL,
    TemperatureControlled INTEGER,
    HazardousMaterial   INTEGER,
    CurrentUtilizationPct REAL,
    ManagerUserID       INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    AccessControlLevel  TEXT
);

-- 57 -------------------------------------------------------------
CREATE TABLE call_center_agents (
    AgentID             INTEGER PRIMARY KEY,
    FirstName           TEXT,
    LastName            TEXT,
    HireDate            DATE,
    TeamID              INTEGER,
    SupervisorUserID    INTEGER,
    EmploymentType      TEXT,
    LanguageSkills      TEXT,
    AverageHandleTimeSec INTEGER,
    SatisfactionScore   REAL,
    CallsHandledToday   INTEGER,
    AttendanceStatus    TEXT,
    ShiftStartTime      TEXT,
    ShiftEndTime        TEXT,
    IsRemote            INTEGER,
    RemoteLocation      TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    TerminationDate     DATE,
    TerminationReason   TEXT
);

-- 58 -------------------------------------------------------------
CREATE TABLE inventory_audits (
    AuditID             INTEGER PRIMARY KEY,
    WarehouseID         INTEGER,
    ZoneID              INTEGER,
    AuditDate           DATE,
    AuditorUserID       INTEGER,
    ItemsCounted        INTEGER,
    DiscrepancyCount    INTEGER,
    DiscrepancyValue    REAL,
    Currency            TEXT,
    Notes               TEXT,
    FollowUpRequired    INTEGER,
    FollowUpDate        DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsClosed            INTEGER,
    ClosureDate         DATE,
    ClosureRemarks      TEXT
);

-- 59 -------------------------------------------------------------
CREATE TABLE external_api_keys (
    ApiKeyID            INTEGER PRIMARY KEY,
    ServiceName         TEXT,
    KeyValue            TEXT,
    OwnerUserID         INTEGER,
    CreatedDate         DATE,
    ExpirationDate      DATE,
    IsActive            INTEGER,
    Permissions         TEXT,
    RateLimitPerMinute  INTEGER,
    LastUsedTimestamp   TEXT,
    LastUsedIP          TEXT,
    RevokedByUserID     INTEGER,
    RevokedDate         DATE,
    RevocationReason    TEXT,
    Notes               TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 60 -------------------------------------------------------------
CREATE TABLE data_mart_mappings (
    MappingID           INTEGER PRIMARY KEY,
    SourceTable         TEXT,
    SourceColumn        TEXT,
    TargetTable         TEXT,
    TargetColumn        TEXT,
    TransformationLogic TEXT,
    IsActive            INTEGER,
    CreatedByUserID     INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    Comments            TEXT,
    VersionNumber       TEXT,
    ValidFromDate       DATE,
    ValidToDate         DATE
);

-- 61 -------------------------------------------------------------
CREATE TABLE smart_device_inventory (
    DeviceID            INTEGER PRIMARY KEY,
    DeviceSerialNumber  TEXT,
    DeviceModel         TEXT,
    Manufacturer        TEXT,
    FirmwareVersion     TEXT,
    InstallationDate    DATE,
    WarrantyEndDate     DATE,
    LocationID          INTEGER,
    AssignedToUserID    INTEGER,
    Status              TEXT,
    LastSeenTimestamp   TEXT,
    BatteryHealthPct    REAL,
    ConnectivityStatus TEXT,
    IPAddress           TEXT,
    MACAddress          TEXT,
    LastMaintenanceDate DATE,
    MaintenanceDueDate  DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsDecommissioned    INTEGER,
    DecommissionDate    DATE
);

-- 62 -------------------------------------------------------------
CREATE TABLE corporate_social_responsibility (
    InitiativeID        INTEGER PRIMARY KEY,
    Name                TEXT,
    Description         TEXT,
    StartDate           DATE,
    EndDate             DATE,
    BudgetAllocated     REAL,
    Currency            TEXT,
    LeadOfficeID        INTEGER,
    StakeholderGroup   TEXT,
    ImpactMetric        TEXT,
    ImpactValue         REAL,
    Status              TEXT,
    PartnerOrganization TEXT,
    ReportURL           TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    ArchiveDate         DATE,
    ArchiveReason       TEXT,
    Notes               TEXT
);

-- 63 -------------------------------------------------------------
CREATE TABLE procurement_contracts (
    ContractID          INTEGER PRIMARY KEY,
    ContractNumber      TEXT,
    SupplierID          INTEGER,
    Category            TEXT,
    StartDate           DATE,
    EndDate             DATE,
    TotalValue          REAL,
    Currency            TEXT,
    PaymentTerms        TEXT,
    DeliveryTerms       TEXT,
    ServiceLevelAgreement TEXT,
    RenewalOption       TEXT,
    SignedByUserID      INTEGER,
    SignedDate          DATE,
    Status              TEXT,
    IsFrameworkAgreement INTEGER,
    FrameworkID         INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsArchived          INTEGER,
    ArchiveDate         DATE,
    ArchiveReason       TEXT
);

-- 64 -------------------------------------------------------------
CREATE TABLE internal_audit_findings (
    FindingID           INTEGER PRIMARY KEY,
    AuditID             INTEGER,
    Description         TEXT,
    RiskLevel           TEXT,
    Impact              TEXT,
    Recommendation      TEXT,
    OwnerUserID         INTEGER,
    DueDate             DATE,
    Status              TEXT,
    ResolutionDate      DATE,
    EvidenceLink        TEXT,
    IsRepeatFinding     INTEGER,
    FollowUpAuditID     INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsClosed            INTEGER,
    ClosureReason       TEXT,
    Comments            TEXT,
    AuditTeamLeadID     INTEGER
);

-- 65 -------------------------------------------------------------
CREATE TABLE marketing_assets (
    AssetID             INTEGER PRIMARY KEY,
    AssetName           TEXT,
    AssetType           TEXT,
    FilePath            TEXT,
    FileSizeBytes       INTEGER,
    CreatedByUserID     INTEGER,
    CreatedDate         DATE,
    ModifiedByUserID    INTEGER,
    ModifiedDate        DATE,
    CampaignID          INTEGER,
    UsageRights         TEXT,
    ExpirationDate      DATE,
    Tags                TEXT,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    VersionNumber       TEXT,
    ApprovalStatus      TEXT,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    Comments            TEXT
);

-- 66 -------------------------------------------------------------
CREATE TABLE real_estate_properties (
    PropertyID          INTEGER PRIMARY KEY,
    PropertyName        TEXT,
    AddressLine1        TEXT,
    AddressLine2        TEXT,
    City                TEXT,
    StateProvince       TEXT,
    PostalCode          TEXT,
    Country             TEXT,
    PropertyType        TEXT,
    BuiltYear           INTEGER,
    GrossFloorAreaSqM   REAL,
    OwnerCompanyID      INTEGER,
    ManagerUserID       INTEGER,
    LeaseStartDate      DATE,
    LeaseEndDate        DATE,
    AnnualRent          REAL,
    Currency            TEXT,
    OccupancyRatePct    REAL,
    IsVacant            INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    Notes               TEXT
);

-- 67 -------------------------------------------------------------
CREATE TABLE climate_data (
    RecordID            INTEGER PRIMARY KEY,
    StationID           TEXT,
    ObservationDate     DATE,
    TemperatureC        REAL,
    PrecipitationMm    REAL,
    WindSpeedKph       REAL,
    WindDirectionDeg   INTEGER,
    HumidityPct        REAL,
    SolarRadiationWm2  REAL,
    SnowDepthCm        REAL,
    CloudCoverPct      REAL,
    VisibilityKm       REAL,
    WeatherCondition   TEXT,
    DataQualityFlag    TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsVerified          INTEGER,
    VerificationDate    DATE,
    VerifiedByUserID    INTEGER
);

-- 68 -------------------------------------------------------------
CREATE TABLE security_incidents (
    IncidentID          INTEGER PRIMARY KEY,
    IncidentDateTime    TEXT,
    ReporterUserID      INTEGER,
    Category            TEXT,
    SeverityLevel       TEXT,
    Description         TEXT,
    AffectedSystem      TEXT,
    ImpactAssessment    TEXT,
    ContainmentAction   TEXT,
    EradicationAction  TEXT,
    RecoveryAction      TEXT,
    RootCauseAnalysis  TEXT,
    LessonsLearned      TEXT,
    Status              TEXT,
    ResolutionDate      DATE,
    AssignedToUserID    INTEGER,
    IsRegulated        INTEGER,
    RegulatoryBody      TEXT,
    PenaltyAmount       REAL,
    Currency            TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsClosed            INTEGER,
    ClosureNotes        TEXT,
    FollowUpDate        DATE
);

-- 69 -------------------------------------------------------------
CREATE TABLE sustainability_metrics (
    MetricID            INTEGER PRIMARY KEY,
    MetricName          TEXT,
    Description         TEXT,
    Unit                TEXT,
    TargetValue         REAL,
    CurrentValue        REAL,
    ReportingPeriod    TEXT,
    DepartmentID        INTEGER,
    OwnerUserID         INTEGER,
    LastUpdatedDate     DATE,
    DataSource          TEXT,
    IsKeyPerformanceIndicator INTEGER,
    Trend               TEXT,
    ConfidenceLevelPct  REAL,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    Comments            TEXT
);

-- 70 -------------------------------------------------------------
CREATE TABLE recruitment_applications (
    ApplicationID       INTEGER PRIMARY KEY,
    CandidateName       TEXT,
    PositionID          INTEGER,
    DepartmentID        INTEGER,
    ApplicationDate    DATE,
    ResumePath          TEXT,
    CoverLetterPath     TEXT,
    Status              TEXT,
    InterviewStage     TEXT,
    InterviewerUserID   INTEGER,
    InterviewDate      DATE,
    OfferExtendedDate  DATE,
    OfferAcceptedDate  DATE,
    SalaryOffered      REAL,
    Currency            TEXT,
    ReferrerEmployeeID INTEGER,
    SourceChannel       TEXT,
    Notes               TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsWithdrawn        INTEGER,
    WithdrawalReason    TEXT
);

-- 71 -------------------------------------------------------------
CREATE TABLE educational_courses (
    CourseID            INTEGER PRIMARY KEY,
    CourseCode          TEXT,
    CourseTitle         TEXT,
    Description         TEXT,
    DepartmentID        INTEGER,
    InstructorUserID    INTEGER,
    CreditHours         REAL,
    Level               TEXT,
    Language            TEXT,
    DeliveryMode        TEXT,
    StartDate           DATE,
    EndDate             DATE,
    EnrollmentCap       INTEGER,
    EnrollmentCount    INTEGER,
    PrerequisiteCourseID INTEGER,
    SyllabusURL        TEXT,
    IsMandatory        INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive           INTEGER,
    DeactivationDate   DATE,
    DeactivationReason TEXT,
    Comments           TEXT
);

-- 72 -------------------------------------------------------------
CREATE TABLE conference_attendees (
    AttendeeID          INTEGER PRIMARY KEY,
    EventID             INTEGER,
    FirstName           TEXT,
    LastName            TEXT,
    CompanyName         TEXT,
    JobTitle            TEXT,
    Email               TEXT,
    Phone               TEXT,
    RegistrationDate    DATE,
    TicketType          TEXT,
    DietaryRestrictions TEXT,
    AccessibilityNeeds  TEXT,
    CheckInStatus      INTEGER,
    CheckInTime        TEXT,
    BadgePrinted        INTEGER,
    SurveyCompleted    INTEGER,
    FeedbackScore       REAL,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsCancelled        INTEGER,
    CancellationReason  TEXT,
    Notes               TEXT
);

-- 73 -------------------------------------------------------------
CREATE TABLE litigation_documents (
    DocumentID          INTEGER PRIMARY KEY,
    CaseID              INTEGER,
    DocumentType        TEXT,
    FilePath            TEXT,
    UploadedByUserID    INTEGER,
    UploadDate          DATE,
    ConfidentialLevel   TEXT,
    VersionNumber       TEXT,
    IsOfficial          INTEGER,
    SignatureRequired   INTEGER,
    SignedByUserID      INTEGER,
    SignatureDate       DATE,
    IsRedacted          INTEGER,
    RedactionReason     TEXT,
    AccessCount         INTEGER,
    LastAccessedDate    DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsArchived          INTEGER,
    ArchiveDate         DATE
);

-- 74 -------------------------------------------------------------
CREATE TABLE cloud_resource_inventory (
    ResourceID          INTEGER PRIMARY KEY,
    Provider            TEXT,
    ServiceType         TEXT,
    ResourceName        TEXT,
    ResourceTag         TEXT,
    Region              TEXT,
    AccountID           TEXT,
    CreationDate        DATE,
    LastModifiedDate    DATE,
    OwnerUserID         INTEGER,
    CostMonthlyUSD      REAL,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    ComplianceStatus    TEXT,
    SecurityGroupID     TEXT,
    BackupEnabled       INTEGER,
    BackupFrequencyDays INTEGER,
    EncryptionAtRest    INTEGER,
    EncryptionInTransit INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 75 -------------------------------------------------------------
CREATE TABLE vendor_invoices (
    InvoiceID           INTEGER PRIMARY KEY,
    SupplierID          INTEGER,
    InvoiceNumber       TEXT,
    InvoiceDate         DATE,
    DueDate             DATE,
    TotalAmount         REAL,
    Currency            TEXT,
    TaxAmount           REAL,
    DiscountAmount      REAL,
    PaidAmount          REAL,
    PaymentDate         DATE,
    PaymentMethod       TEXT,
    Status              TEXT,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    AttachmentsPath     TEXT,
    Notes               TEXT,
    IsLatePayment       INTEGER,
    LateFeeAmount       REAL,
    CreatedAt           TEXT,
    UpdatedAt           TEXT
);

-- 76 -------------------------------------------------------------
CREATE TABLE university_enrollments (
    EnrollmentID        INTEGER PRIMARY KEY,
    StudentID           INTEGER,
    ProgramID           INTEGER,
    EnrollmentDate      DATE,
    ExpectedGraduationDate DATE,
    CurrentTerm         TEXT,
    AcademicStatus      TEXT,
    GPA                 REAL,
    CreditsEarned       INTEGER,
    AdvisorUserID       INTEGER,
    ScholarshipAmount   REAL,
    ScholarshipCurrency TEXT,
    FinancialAidStatus  TEXT,
    IsInternational     INTEGER,
    CountryOfOrigin     TEXT,
    HousingAssigned     INTEGER,
    DormRoomNumber      TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT
);

-- 77 -------------------------------------------------------------
CREATE TABLE product_warranty_claims (
    ClaimID             INTEGER PRIMARY KEY,
    WarrantyID          INTEGER,
    ProductSerialNumber TEXT,
    ClaimDate           DATE,
    IssueDescription    TEXT,
    RequestedRemedy     TEXT,
    ClaimStatus         TEXT,
    ApprovedAmount      REAL,
    Currency            TEXT,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    ResolutionDate      DATE,
    ResolutionNotes     TEXT,
    IsRefund            INTEGER,
    IsReplacement       INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsEscalated         INTEGER,
    EscalationReason    TEXT,
    SupportingDocumentsPath TEXT
);

-- 78 -------------------------------------------------------------
CREATE TABLE training_sessions (
    SessionID           INTEGER PRIMARY KEY,
    CourseID            INTEGER,
    SessionDate         DATE,
    StartTime           TEXT,
    EndTime             TEXT,
    TrainerUserID       INTEGER,
    Location            TEXT,
    MaxAttendees        INTEGER,
    RegisteredAttendees INTEGER,
    MaterialsPath       TEXT,
    FeedbackScoreAvg    REAL,
    IsMandatory         INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsCancelled         INTEGER,
    CancellationReason  TEXT,
    Notes               TEXT
);

-- 79 -------------------------------------------------------------
CREATE TABLE financial_forecasts (
    ForecastID          INTEGER PRIMARY KEY,
    ForecastYear        INTEGER,
    ForecastMonth       INTEGER,
    RevenueForecast     REAL,
    CostForecast        REAL,
    EBITDAForecast      REAL,
    CapitalExpenditure  REAL,
    Currency            TEXT,
    AssumptionsDocumentPath TEXT,
    PreparedByUserID    INTEGER,
    PreparationDate     DATE,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    Status              TEXT,
    VersionNumber       TEXT,
    Comments            TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsFinal             INTEGER,
    FinalizationDate    DATE
);

-- 80 -------------------------------------------------------------
CREATE TABLE customer_support_tickets (
    TicketID            INTEGER PRIMARY KEY,
    CustomerID          INTEGER,
    IssueCategory       TEXT,
    IssueSubCategory    TEXT,
    Subject             TEXT,
    Description         TEXT,
    Priority            TEXT,
    Status              TEXT,
    OpenedDate          DATE,
    ClosedDate          DATE,
    AssignedAgentID     INTEGER,
    SLAHours            REAL,
    ResolutionTimeHours REAL,
    SatisfactionScore   REAL,
    FollowUpRequired    INTEGER,
    FollowUpDate        DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsEscalated         INTEGER,
    EscalationLevel     TEXT,
    ExternalReference   TEXT
);

-- 81 -------------------------------------------------------------
CREATE TABLE IoT_device_firmware (
    FirmwareID          INTEGER PRIMARY KEY,
    DeviceModel         TEXT,
    FirmwareVersion     TEXT,
    ReleaseDate         DATE,
    ReleaseNotes        TEXT,
    BinaryFilePath      TEXT,
    ChecksumSHA256      TEXT,
    IsSigned            INTEGER,
    SignatureKeyID      TEXT,
    MinimumHardwareVersion TEXT,
    SupportedOS         TEXT,
    Deprecated          INTEGER,
    DeprecationDate     DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    IsBetaRelease       INTEGER,
    BetaExpirationDate  DATE
);

-- 82 -------------------------------------------------------------
CREATE TABLE litigation_cases (
    CaseID              INTEGER PRIMARY KEY,
    CaseNumber          TEXT,
    PlaintiffName       TEXT,
    DefendantName       TEXT,
    FilingDate          DATE,
    Court               TEXT,
    Judge               TEXT,
    CaseStatus          TEXT,
    EstimatedValue      REAL,
    Currency            TEXT,
    SettlementAmount    REAL,
    SettlementDate      DATE,
    AttorneyID          INTEGER,
    LeadCounselID       INTEGER,
    Jurisdiction        TEXT,
    Confidential        INTEGER,
    DocumentRepositoryPath TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsClosed            INTEGER,
    ClosingDate         DATE,
    CloseReason         TEXT
);

-- 83 -------------------------------------------------------------
CREATE TABLE procurement_tenders (
    TenderID            INTEGER PRIMARY KEY,
    TenderNumber        TEXT,
    Title               TEXT,
    Description         TEXT,
    DepartmentID        INTEGER,
    IssueDate           DATE,
    ClosingDate         DATE,
    BudgetAmount        REAL,
    Currency            TEXT,
    EvaluationMethod    TEXT,
    Status              TEXT,
    WinningSupplierID   INTEGER,
    ContractID          INTEGER,
    AttachmentsPath     TEXT,
    CreatedByUserID     INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsCancelled         INTEGER,
    CancellationReason  TEXT,
    Notes               TEXT
);

-- 84 -------------------------------------------------------------
CREATE TABLE medical_devices (
    DeviceID            INTEGER PRIMARY KEY,
    SerialNumber        TEXT,
    Model               TEXT,
    Manufacturer        TEXT,
    PurchaseDate        DATE,
    WarrantyEndDate     DATE,
    LocationID          INTEGER,
    AssignedToUserID    INTEGER,
    CalibrationDate     DATE,
    NextCalibrationDue  DATE,
    Status              TEXT,
    MaintenanceLogPath  TEXT,
    IsRegulated         INTEGER,
    RegulatoryClass     TEXT,
    IsRecall            INTEGER,
    RecallDate          DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsDecommissioned    INTEGER,
    DecommissionDate    DATE
);

-- 85 -------------------------------------------------------------
CREATE TABLE supplier_contacts (
    ContactID           INTEGER PRIMARY KEY,
    SupplierID          INTEGER,
    ContactName         TEXT,
    Role                TEXT,
    Email               TEXT,
    Phone               TEXT,
    MobilePhone         TEXT,
    PreferredContactMethod TEXT,
    IsPrimary           INTEGER,
    Language            TEXT,
    Timezone            TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    Notes               TEXT
);

-- 86 -------------------------------------------------------------
CREATE TABLE grant_applications (
    ApplicationID       INTEGER PRIMARY KEY,
    GrantProgramID      INTEGER,
    ApplicantOrganizationID INTEGER,
    ProjectTitle        TEXT,
    Abstract            TEXT,
    RequestedAmount     REAL,
    Currency            TEXT,
    SubmissionDate      DATE,
    ReviewScore         REAL,
    ReviewComments      TEXT,
    Status              TEXT,
    AwardedAmount       REAL,
    AwardDate           DATE,
    ReportingPeriod     TEXT,
    FinalReportPath     TEXT,
    CreatedByUserID     INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsWithdrawn        INTEGER,
    WithdrawalReason    TEXT,
    Notes               TEXT
);

-- 87 -------------------------------------------------------------
CREATE TABLE corporate_trainings (
    TrainingID          INTEGER PRIMARY KEY,
    Title               TEXT,
    Description         TEXT,
    DeliveryMethod      TEXT,
    DurationHours       REAL,
    TargetAudience      TEXT,
    MandatoryForDeptIDs TEXT,
    TrainerUserID       INTEGER,
    StartDate           DATE,
    EndDate             DATE,
    Location            TEXT,
    Capacity            INTEGER,
    EnrolledCount       INTEGER,
    PassRatePct         REAL,
    EvaluationScoreAvg  REAL,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    Comments            TEXT
);

-- 88 -------------------------------------------------------------
CREATE TABLE digital_ad_spends (
    SpendID             INTEGER PRIMARY KEY,
    CampaignID          INTEGER,
    Platform            TEXT,
    Date                DATE,
    Impressions         INTEGER,
    Clicks              INTEGER,
    Conversions         INTEGER,
    SpendAmount         REAL,
    Currency            TEXT,
    CostPerMile         REAL,
    CostPerClick        REAL,
    CostPerConversion   REAL,
    ROI                 REAL,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsFinalized         INTEGER,
    FinalizationDate    DATE,
    Notes               TEXT,
    AttributionModel    TEXT,
    ClickThroughRate    REAL
);

-- 89 -------------------------------------------------------------
CREATE TABLE compliance_training_records (
    RecordID            INTEGER PRIMARY KEY,
    EmployeeID          INTEGER,
    TrainingID          INTEGER,
    CompletionDate      DATE,
    Score               REAL,
    Passed              INTEGER,
    CertificatePath    TEXT,
    TrainerUserID       INTEGER,
    ExpirationDate      DATE,
    IsExpired           INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    Remarks             TEXT,
    IsRecertificationRequired INTEGER,
    NextDueDate         DATE,
    CertificationNumber TEXT
);

-- 90 -------------------------------------------------------------
CREATE TABLE public_transport_routes (
    RouteID             INTEGER PRIMARY KEY,
    RouteNumber         TEXT,
    RouteName           TEXT,
    ServiceType         TEXT,
    OperatingHours      TEXT,
    FrequencyMinutes    INTEGER,
    PrimaryVehicleType  TEXT,
    TotalStops          INTEGER,
    AverageTravelTimeMin INTEGER,
    PeakHourCapacity   INTEGER,
    OffPeakCapacity    INTEGER,
    OperatorCompanyID   INTEGER,
    IsActive            INTEGER,
    ActivationDate      DATE,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    MapFilePath         TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    Notes               TEXT
);

-- 91 -------------------------------------------------------------
CREATE TABLE staff_shift_schedules (
    ShiftID             INTEGER PRIMARY KEY,
    EmployeeID          INTEGER,
    ShiftDate           DATE,
    ShiftStartTime      TEXT,
    ShiftEndTime        TEXT,
    ShiftType           TEXT,
    DepartmentID        INTEGER,
    LocationID          INTEGER,
    IsOvertime          INTEGER,
    OvertimeHours       REAL,
    BreakDurationMin    INTEGER,
    AssignedSupervisorID INTEGER,
    IsConfirmed         INTEGER,
    ConfirmationDate    DATE,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsCancelled         INTEGER,
    CancellationReason  TEXT,
    Notes               TEXT
);

-- 92 -------------------------------------------------------------
CREATE TABLE renewable_energy_assets (
    AssetID            INTEGER PRIMARY KEY,
    AssetName          TEXT,
    AssetType          TEXT,
    CapacityMW         REAL,
    InstallationDate   DATE,
    CommissionDate     DATE,
    OwnerCompanyID     INTEGER,
    LocationID         INTEGER,
    CurrentGenerationMW REAL,
    OperatingStatus    TEXT,
    MaintenanceDueDate DATE,
    IsGridConnected    INTEGER,
    SubsidyAmount      REAL,
    Currency           TEXT,
    ExpectedLifetimeYears INTEGER,
    CreatedAt          TEXT,
    UpdatedAt          TEXT,
    IsDecommissioned   INTEGER,
    DecommissionDate   DATE,
    DecommissionReason TEXT
);

-- 93 -------------------------------------------------------------
CREATE TABLE emergency_contacts (
    ContactID           INTEGER PRIMARY KEY,
    EmployeeID          INTEGER,
    ContactName         TEXT,
    Relationship        TEXT,
    PhoneNumber         TEXT,
    Email               TEXT,
    PreferredContactMethod TEXT,
    IsPrimary           INTEGER,
    AddressLine1        TEXT,
    AddressLine2        TEXT,
    City                TEXT,
    StateProvince       TEXT,
    PostalCode          TEXT,
    Country             TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    Notes               TEXT
);

-- 94 -------------------------------------------------------------
CREATE TABLE HR_policies (
    PolicyID            INTEGER PRIMARY KEY,
    PolicyName          TEXT,
    Category            TEXT,
    EffectiveDate       DATE,
    ExpirationDate      DATE,
    OwnerDepartmentID   INTEGER,
    DocumentPath        TEXT,
    RevisionNumber      TEXT,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    Status              TEXT,
    IsMandatory         INTEGER,
    TrainingRequired    INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsArchived          INTEGER,
    ArchiveDate         DATE,
    ArchiveReason       TEXT,
    Notes               TEXT
);

-- 95 -------------------------------------------------------------
CREATE TABLE product_bundle_offers (
    BundleID            INTEGER PRIMARY KEY,
    BundleName          TEXT,
    Description         TEXT,
    StartDate           DATE,
    EndDate             DATE,
    DiscountPercentage  REAL,
    Currency            TEXT,
    BundlePrice         REAL,
    IncludedProductIDs  TEXT,
    EligibilityCriteria TEXT,
    IsActive            INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsFeatured          INTEGER,
    FeaturedStartDate   DATE,
    FeaturedEndDate     DATE,
    ImagePath           TEXT,
    TermsAndConditions  TEXT
);

-- 96 -------------------------------------------------------------
CREATE TABLE chatbot_conversations (
    ConversationID      INTEGER PRIMARY KEY,
    SessionID           TEXT,
    UserID              INTEGER,
    StartTimestamp      TEXT,
    EndTimestamp        TEXT,
    MessageCount        INTEGER,
    IntentDetected      TEXT,
    SentimentScore      REAL,
    EscalatedToHuman   INTEGER,
    AgentUserID         INTEGER,
    ResolutionStatus    TEXT,
    FeedbackScore       REAL,
    Language            TEXT,
    Platform            TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsClosed            INTEGER,
    ClosureReason       TEXT,
    Notes               TEXT
);

-- 97 -------------------------------------------------------------
CREATE TABLE procurement_approval_workflows (
    WorkflowID          INTEGER PRIMARY KEY,
    WorkflowName        TEXT,
    Description         TEXT,
    InitiatorUserID     INTEGER,
    DepartmentID        INTEGER,
    ApprovalLevelCount  INTEGER,
    CurrentStepNumber   INTEGER,
    Status              TEXT,
    CreatedAt           DATE,
    UpdatedAt           DATE,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    Notes               TEXT,
    EscalationPolicyID  INTEGER,
    LastStepCompletedByUserID INTEGER,
    LastStepCompletedDate DATE
);

-- 98 -------------------------------------------------------------
CREATE TABLE supplier_certifications (
    CertificationID     INTEGER PRIMARY KEY,
    SupplierID          INTEGER,
    CertificationName   TEXT,
    CertificationBody   TEXT,
    IssueDate           DATE,
    ExpirationDate      DATE,
    CertificationNumber TEXT,
    IsActive            INTEGER,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    AttachedDocumentPath TEXT,
    Remarks             TEXT,
    RenewalReminderDays INTEGER,
    LastRenewalDate     DATE,
    NextRenewalDate     DATE,
    IsVerified          INTEGER,
    VerifiedByUserID    INTEGER,
    VerificationDate    DATE
);

-- 99 -------------------------------------------------------------
CREATE TABLE university_research_projects (
    ProjectID           INTEGER PRIMARY KEY,
    Title               TEXT,
    PrincipalInvestigatorUserID INTEGER,
    DepartmentID        INTEGER,
    FundingAgency       TEXT,
    GrantNumber         TEXT,
    StartDate           DATE,
    EndDate             DATE,
    TotalBudget         REAL,
    Currency            TEXT,
    Status              TEXT,
    PublicationsCount   INTEGER,
    PatentsFiledCount  INTEGER,
    CollaborationPartners TEXT,
    DataRepositoryURL   TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    Notes               TEXT
);

-- 100 ------------------------------------------------------------
CREATE TABLE data_retention_policies (
    PolicyID            INTEGER PRIMARY KEY,
    PolicyName          TEXT,
    Description         TEXT,
    EffectiveDate       DATE,
    ExpirationDate      DATE,
    RetentionPeriodDays INTEGER,
    ApplicableDataDomain TEXT,
    OwnerDepartmentID   INTEGER,
    ApprovalStatus      TEXT,
    ApprovedByUserID    INTEGER,
    ApprovalDate        DATE,
    LastReviewDate      DATE,
    ReviewFrequencyDays INTEGER,
    IsActive            INTEGER,
    DeactivationDate    DATE,
    DeactivationReason  TEXT,
    DocumentationURL    TEXT,
    CreatedAt           TEXT,
    UpdatedAt           TEXT,
    Notes               TEXT
);