SET DATESTYLE TO YMD;
CREATE TABLE Employee (
  SSN char(9) NOT NULL,
  FName varchar(15) NOT NULL,
  MInit char(1),
  LName varchar(15) NOT NULL,
  BirthDate date NULL,
  Sex char(1),
  Lifespan datespanset,
  CONSTRAINT PK_Employee PRIMARY KEY (SSN)
);

CREATE TABLE EmpSalary (
  SSN char(9) NOT NULL,
  Salary int,
  VT datespanset,
  CONSTRAINT PK_EmpSalary PRIMARY KEY (SSN,Salary),
  CONSTRAINT FK_EmpSalary_Employee FOREIGN KEY (SSN) REFERENCES Employee (SSN)
);

CREATE TABLE EmpAddress (
  SSN char(9) NOT NULL,
  Street varchar(20),
  City varchar(20),
  Zip varchar(10),
  State varchar(10),
  VT datespanset NOT NULL,
  -- CONSTRAINT PK_EmpAddress PRIMARY KEY (SSN),
  CONSTRAINT FK_EmpAddress_Employee FOREIGN KEY (SSN) REFERENCES Employee (SSN)
);

CREATE TABLE Supervision (
  SSN char(9) NOT NULL,
  SuperSSN char(9) NOT NULL,
  VT datespanset NOT NULL,
  CONSTRAINT PK_Supervision PRIMARY KEY (SSN,SuperSSN),
  CONSTRAINT FK_Supervision_Employee_Emp FOREIGN KEY (SSN) REFERENCES Employee (SSN),
  CONSTRAINT FK_Supervision_Employee_Super FOREIGN KEY (SuperSSN) REFERENCES Employee (SSN)
);

CREATE TABLE Department (
  DNumber int NOT NULL,
  DName varchar(15) NOT NULL,
  MgrSSN char(9) NOT NULL,
  MgrStartDate date,
  VT datespanset NOT NULL,
  CONSTRAINT PK_Department PRIMARY KEY (DNumber),
  CONSTRAINT FK_Department_Employee FOREIGN KEY (MgrSSN) REFERENCES Employee (SSN)
    ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE Affiliation (
  SSN char(9) NOT NULL,
  DNumber int NOT NULL,
  VT datespanset NOT NULL,
  CONSTRAINT PK_Affiliation PRIMARY KEY (SSN,DNumber),
  CONSTRAINT FK_Affiliation_Employee FOREIGN KEY (SSN) REFERENCES Employee (SSN),
  CONSTRAINT FK_Affiliation_Department FOREIGN KEY (DNumber) REFERENCES Department (DNumber)
);

CREATE TABLE DeptNbEmp (
  DNumber int NOT NULL,
  NbEmp int,
  VT datespanset NOT NULL,
  CONSTRAINT PK_DepartmentNbEmp PRIMARY KEY (DNumber,NbEmp),
  CONSTRAINT FK_DepartmentNbEmp_Department FOREIGN KEY (DNumber) REFERENCES Department (DNumber)
    ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE DeptLocations (
  DNumber int NOT NULL,
  DLocation varchar(15) NOT NULL,
  VT datespanset NOT NULL,
  CONSTRAINT PK_DeptLocations PRIMARY KEY (DNumber,DLocation),
  CONSTRAINT FK_DeptLocations_Department FOREIGN KEY (DNumber) REFERENCES Department (DNumber)
);

CREATE TABLE Project (
  PNumber int NOT NULL,
  PName varchar(15) NOT NULL,
  PLocation varchar(15),
  VT datespanset NOT NULL,
  CONSTRAINT PK_Project PRIMARY KEY (PNumber)
);

CREATE TABLE Controls (
  PNumber int NOT NULL,
  DNumber int NOT NULL,
  VT datespanset NOT NULL,
  CONSTRAINT PK_Controls PRIMARY KEY (PNumber,DNumber),
  CONSTRAINT FK_Controls_Department FOREIGN KEY (DNumber) REFERENCES Department (DNumber)
);

CREATE TABLE WorksOn (
  SSN char(9) NOT NULL,
  PNumber int NOT NULL,
  Hours decimal(18,1) NOT NULL,
  VT datespanset NOT NULL,
  CONSTRAINT PK_WorksOn PRIMARY KEY (SSN,PNumber),
  CONSTRAINT FK_WorksOn_Employee FOREIGN KEY (SSN) REFERENCES Employee (SSN),
  CONSTRAINT FK_WorksOn_Project FOREIGN KEY (PNumber) REFERENCES Project (PNumber)
);
