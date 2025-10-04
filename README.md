# Databricks Platform Assignment - Van Oord

This repository contains the complete implementation for the Databricks platform assignment, demonstrating enterprise-grade data platform capabilities with Unity Catalog governance, Power BI integration, and best practices for data engineering.

## 🏗️ Repository Structure

```text
├── databricks_setup/          # Terraform infrastructure as code
│   ├── main.tf               # Main Databricks workspace configuration
│   ├── variables.tf          # Terraform variables
│   ├── outputs.tf            # Terraform outputs
│   └── terraform.tfvars      # Environment-specific values
├── notebooks/                 # Databricks notebooks and demos
│   ├── data_ingestion/       # CSV to Parquet processing
│   ├── delta_demo/           # Delta table versioning
│   ├── incremental_pipeline/ # Incremental processing
│   ├── query_optimization/   # Performance optimization
│   └── quality_checks/       # Data quality integration
├── sql/                      # SQL scripts and queries
├── docs/                     # Documentation and best practices
├── .pre-commit-config.yaml   # Code quality hooks
└── README.md                 # This file
```

## 🚀 Quick Start

### Prerequisites

- Azure CLI installed and authenticated (`az login`)
- Terraform >= 1.0
- Python >= 3.8
- Git

### 1. Infrastructure Setup

```bash
# Clone and navigate to the repository
git clone <repository-url>
cd vanOord_assignment

# Initialize Terraform
cd databricks_setup
terraform init

# Plan the deployment
terraform plan

# Deploy the infrastructure
terraform apply
```

### 2. Code Quality Setup

#### **Install Pre-commit Hooks:**

```bash
# Install pre-commit using pipx (recommended)
brew install pipx
pipx install pre-commit
pipx ensurepath

# Install hooks in the repository
pre-commit install

# Run hooks on all files to check current state
pre-commit run --all-files
```

#### **Pre-commit Hooks Configuration:**

The repository includes comprehensive pre-commit hooks configured in `.pre-commit-config.yaml`:

- **✅ General Checks**: Large files, merge conflicts, YAML/JSON/TOML/XML validation
- **✅ File Formatting**: End-of-file fixer, trailing whitespace, mixed line endings
- **✅ Python Quality**: Black formatting, Flake8 linting, isort import organization
- **✅ Terraform**: Formatting, validation, documentation, tflint
- **✅ Security**: Secret detection with baseline
- **✅ Documentation**: Markdown linting
- **✅ Notebooks**: Jupyter notebook cleanup

#### **Automatic Execution:**

Pre-commit hooks run automatically on every `git commit`:

```bash
# Normal workflow - hooks run automatically
git add .
git commit -m "Your changes"
# ↓ Pre-commit hooks run automatically
# ↓ If issues found, commit is blocked
# ↓ Fix issues and commit again

# Emergency bypass (use sparingly)
git commit --no-verify -m "Skip pre-commit hooks"
```

#### **Manual Execution:**

```bash
# Run all hooks on all files
pre-commit run --all-files

# Run hooks on specific files
pre-commit run --files README.md notebooks/

# Run specific hook
pre-commit run flake8
pre-commit run terraform_fmt
```

### 3. Access Databricks Workspace

After deployment, access your workspace at the URL provided in the Terraform output.

### 4. Live Demo Instructions (Platform Assignment)

For the presentation, you can demonstrate live Terraform changes:

```bash
# Option 1: Modify terraform.tfvars file
# Edit databricks_setup/terraform.tfvars and change:
sql_warehouse_cluster_size = "Large"
sql_warehouse_auto_stop_mins = 5

# Option 2: Override variables directly
terraform apply -var="sql_warehouse_cluster_size=Large" -var="sql_warehouse_auto_stop_mins=5"

# Plan the changes
cd databricks_setup
terraform plan

# Apply during presentation
terraform apply
```

**Available SQL Warehouse Sizes for Demo**:

- `2X-Small` (default) → `X-Small` → `Small` → `Medium` → `Large` → `X-Large` → `2X-Large`

**Current Workspace**: <https://adb-734420944977024.4.azuredatabricks.net>

## 🎯 Current Status

### ✅ Successfully Deployed

- **Databricks Workspace**: <https://adb-734420944977024.4.azuredatabricks.net>
- **Storage Account**: voodatabricks77284 (ADLS Gen2)
- **Service Principal**: Configured for Unity Catalog
- **SQL Warehouse**: demo-sql-warehouse (2X-Small)

### 📚 Completed Documentation

- **Technical Answers**: 10 comprehensive sections with code examples
- **Power BI Design**: Complete self-service architecture for 5 departments
- **Infrastructure**: Production-ready Terraform configuration
- **Notebooks**: 4 demonstration notebooks with working code

### 🚀 Ready for Presentation

- **Single Terraform Module**: Successfully deployed as one cohesive unit
- **Live Terraform modification capability**: Can modify SQL warehouse settings during demo
- **Working data pipeline demonstrations**: Complete infrastructure ready
- **Complete governance and security framework**: Unity Catalog with service principal authentication
- **Enterprise code quality**: Pre-commit hooks demonstrate automated quality enforcement

## 📋 Assignment Components

### Platform Assignment ✅

- [x] Terraform module for Azure Databricks workspace deployment
- [x] Git-based configuration management with pre-commit hooks
- [x] Unity Catalog setup with proper authentication
- [x] Infrastructure as Code best practices
- [x] Automated code quality enforcement (Black, Flake8, isort, Terraform validation)
- [x] Security scanning and secret detection

### Databricks Technical Questions ✅

- [x] Cluster sizing, autoscaling, and runtime version decisions
- [x] Databricks Asset Bundle usage
- [x] Secure ADLS Gen2 connectivity
- [x] CSV to Parquet data processing
- [x] Delta table versioning and time travel
- [x] Incremental pipeline design
- [x] Query performance optimization
- [x] Quality check integration (Great Expectations/Deequ)
- [x] Unity Catalog best practices

### Power BI Assessment ✅

- [x] Self-service BI implementation across departments
- [x] Data access control and governance
- [x] Workspace and compute organization
- [x] Role-based access for different teams
- [x] Scalability and maintainability considerations

## 🔧 Infrastructure Components

### Azure Resources

- **Resource Group**: Centralized resource management
- **Databricks Workspace**: Premium tier with Unity Catalog
- **Storage Account**: ADLS Gen2 with organized containers
- **Access Connector**: Secure managed identity authentication
- **SQL Warehouse**: Serverless compute for analytics

### Unity Catalog Setup

- **Metastore**: Centralized metadata management
- **Storage Credentials**: Secure access to ADLS Gen2
- **External Locations**: Organized data access points
- **Grants**: Role-based access control

## 📊 Data Architecture

### Data Layers

- **Raw Layer**: Landing zone for source data
- **Bronze Layer**: Raw data with basic validation
- **Silver Layer**: Cleaned and standardized data
- **Gold Layer**: Business-ready aggregated data

### Access Patterns

- **Analytics Engineers**: Full access to raw/bronze layers
- **Data Scientists**: Access to silver/gold layers
- **Business Users**: Read-only access to gold layer
- **Power BI**: Direct access to curated datasets

## 🔒 Security & Governance

### Authentication

- Azure AD integration
- Managed Identity for service accounts
- Access Connector for secure storage access

### Authorization

- Unity Catalog permissions
- Role-based access control
- Data lineage tracking
- Audit logging

### Compliance

- Data classification
- Retention policies
- Privacy controls
- Audit trails

## 🎯 Best Practices Implemented

### Code Quality

- **Pre-commit hooks**: Automated formatting, linting, and validation on every commit
- **Terraform best practices**: Formatting, validation, documentation, and tflint
- **Python code standards**: Black formatting, Flake8 linting, isort import organization
- **Security scanning**: Secret detection with baseline management
- **Documentation standards**: Markdown linting and consistency checks

### Data Engineering

- Incremental processing patterns
- Delta table optimization
- Data quality monitoring
- Performance tuning

### DevOps

- Infrastructure as Code
- Git-based workflows
- Automated testing
- Environment management

## 📚 Documentation

- [Deployment Success Guide](SERVICE_PRINCIPAL_SUCCESS.md)
- [Technical Questions Answers](docs/technical_answers.md)
- [Power BI Assessment Design](docs/powerbi_assessment_design.md)
- [Notebook Demonstrations](notebooks/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run pre-commit hooks
5. Submit a pull request

## 📄 License

This project is part of the Van Oord Databricks assignment.

## 🆘 Support

For questions or issues, please refer to the documentation or create an issue in the repository.
