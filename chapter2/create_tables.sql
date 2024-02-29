SET DATESTYLE TO YMD;
CREATE TABLE Employee (
  SSN char(9) NOT NULL,
  FName varchar(15) NOT NULL,
  MInit char(1),
  LName varchar(15) NOT NULL,
  BirthDate date NULL,
  Sex char(1),
  CONSTRAINT PK_Employee PRIMARY KEY (SSN)
);

CREATE TABLE EmpLifespan (
  SSN char(9) NOT NULL,
  FromDate date NOT NULL DEFAULT current_date,
  ToDate date NOT NULL DEFAULT '9999-12-31',
  CONSTRAINT PK_EmpLifespan PRIMARY KEY (SSN,FromDate),
  CONSTRAINT FK_EmpLifespan_Employee FOREIGN KEY (SSN) REFERENCES Employee (SSN),
  CONSTRAINT Period_EmpLifespan CHECK (FromDate < ToDate)
);

CREATE TABLE EmpSalary (
  SSN char(9) NOT NULL,
  Salary decimal(18,2),
  FromDate date NOT NULL,
  ToDate date NOT NULL DEFAULT '9999-12-31',
  CONSTRAINT PK_EmpSalary PRIMARY KEY (SSN,FromDate),
  CONSTRAINT FK_EmpSalary_Employee FOREIGN KEY (SSN) REFERENCES Employee (SSN),
  CONSTRAINT Period_EmpSalary CHECK (FromDate < ToDate)
);

CREATE TABLE EmpAddress (
  SSN char(9) NOT NULL,
  Street varchar(20),
  City varchar(20),
  Zip varchar(10),
  State varchar(10),
  FromDate date NOT NULL,
  ToDate date NOT NULL DEFAULT '9999-12-31',
  CONSTRAINT PK_EmpAddress PRIMARY KEY (SSN,FromDate),
  CONSTRAINT FK_EmpAddress_Employee FOREIGN KEY (SSN) REFERENCES Employee (SSN),
  CONSTRAINT Period_EmpAddress CHECK (FromDate < ToDate)
);

CREATE TABLE Supervision (
  SSN char(9) NOT NULL,
  SuperSSN char(9) NOT NULL,
  FromDate date NOT NULL,
  ToDate date NOT NULL DEFAULT '9999-12-31',
  CONSTRAINT PK_Supervision PRIMARY KEY (SSN,SuperSSN,FromDate),
  CONSTRAINT FK_Supervision_Employee_Emp FOREIGN KEY (SSN) REFERENCES Employee (SSN),
  CONSTRAINT FK_Supervision_Employee_Super FOREIGN KEY (SuperSSN) REFERENCES Employee (SSN),
  CONSTRAINT Period_Supervision CHECK (FromDate < ToDate)
);

CREATE TABLE Department (
  DNumber int NOT NULL,
  DName varchar(15) NOT NULL,
  MgrSSN char(9) NOT NULL,
  MgrStartDate date,
  FromDate date NOT NULL,
  ToDate date NOT NULL DEFAULT '9999-12-31',
  CONSTRAINT PK_Department PRIMARY KEY (DNumber),
  CONSTRAINT FK_Department_Employee FOREIGN KEY (MgrSSN) REFERENCES Employee (SSN)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT Period_Department CHECK (FromDate < ToDate)
);

CREATE TABLE Affiliation (
  SSN char(9) NOT NULL,
  DNumber int NOT NULL,
  FromDate date NOT NULL,
  ToDate date NOT NULL DEFAULT '9999-12-31',
  CONSTRAINT PK_Affiliation PRIMARY KEY (SSN,FromDate),
  CONSTRAINT FK_Affiliation_Employee FOREIGN KEY (SSN) REFERENCES Employee (SSN),
  CONSTRAINT FK_Affiliation_Department FOREIGN KEY (DNumber) REFERENCES Department (DNumber),
  CONSTRAINT Period_Affiliation CHECK (FromDate < ToDate)
);

CREATE TABLE DeptNbEmp (
  DNumber int NOT NULL,
  NbEmp int,
  FromDate date NOT NULL,
  ToDate date NOT NULL DEFAULT '9999-12-31',
  CONSTRAINT PK_DepartmentNbEmp PRIMARY KEY (DNumber,FromDate),
  CONSTRAINT FK_DepartmentNbEmp_Department FOREIGN KEY (DNumber) REFERENCES Department (DNumber)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT Period_DeptNbEmp CHECK (FromDate < ToDate)
);

CREATE TABLE DeptLocations (
  DNumber int NOT NULL,
  DLocation varchar(15) NOT NULL,
  FromDate date NOT NULL,
  ToDate date NOT NULL DEFAULT '9999-12-31',
  CONSTRAINT PK_DeptLocations PRIMARY KEY (DNumber,DLocation,FromDate),
  CONSTRAINT FK_DeptLocations_Department FOREIGN KEY (DNumber) REFERENCES Department (DNumber),
  CONSTRAINT Period_DeptLocations CHECK (FromDate < ToDate)
);

CREATE TABLE Project (
  PNumber int NOT NULL,
  PName varchar(15) NOT NULL,
  PLocation varchar(15),
  FromDate date NOT NULL,
  ToDate date NOT NULL DEFAULT '9999-12-31',
  CONSTRAINT PK_Project PRIMARY KEY (PNumber),
  CONSTRAINT Period_Project CHECK (FromDate < ToDate)
);

CREATE TABLE Controls (
  PNumber int NOT NULL,
  DNumber int NOT NULL,
  FromDate date NOT NULL,
  ToDate date NOT NULL DEFAULT '9999-12-31',
  CONSTRAINT PK_Controls PRIMARY KEY (PNumber,FromDate),
  CONSTRAINT FK_Controls_Department FOREIGN KEY (DNumber) REFERENCES Department (DNumber),
  CONSTRAINT Period_Controls CHECK (FromDate < ToDate)
);

CREATE TABLE WorksOn (
  SSN char(9) NOT NULL,
  PNumber int NOT NULL,
  Hours decimal(18,1) NOT NULL,
  FromDate date NOT NULL,
  ToDate date NOT NULL DEFAULT '9999-12-31',
  CONSTRAINT PK_WorksOn PRIMARY KEY (SSN,PNumber,FromDate),
  CONSTRAINT FK_WorksOn_Employee FOREIGN KEY (SSN) REFERENCES Employee (SSN),
  CONSTRAINT FK_WorksOn_Project FOREIGN KEY (PNumber) REFERENCES Project (PNumber),
  CONSTRAINT Period_WorksOn CHECK (FromDate < ToDate)
);
