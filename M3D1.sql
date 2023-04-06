CREATE DATABASE esercizi_1;
USE esercizi_1;
DROP TABLE IF EXISTS Automobile;
CREATE TABLE Automobile(marca varchar(50),modello varchar(35), targa varchar(7), cilindrata int, colore varchar(50));
INSERT INTO Automobile(marca, modello, targa, cilindrata, colore) VALUES ("Ferrari","488","AA123BB",3902,"Rosso");
INSERT INTO Automobile VALUES ("Mercedes","Classe C","CC456DD",2998,"Grigio");
INSERT INTO Automobile (modello, targa, marca, colore, cilindrata) VALUES ("i8","EE789FF","BMW","Blu",1498);
INSERT INTO Automobile VALUES ("Fiat", NULL, NULL, 1233, NULL);
DROP TABLE IF EXISTS Lavoratore;
CREATE TABLE Lavoratore(nome varchar(25), cognome varchar(40), matricola varchar(16), stipendio int);
INSERT INTO Lavoratore VALUES ("Paolo","Verde","A01234",1865), ("Stefano","Neri","B05678",2000), ("Francesco","Azzurra","C09012",2500), ("Luca","Rosso","B091234",2100);
SELECT * FROM Lavoratore WHERE cognome="Verde";
SELECT * FROM Lavoratore WHERE cognome LIKE "%e";
SELECT nome, cognome, stipendio FROM Lavoratore WHERE cognome LIKE "a%";
SELECT * FROM Lavoratore WHERE nome LIKE "%o" AND cognome LIKE "%e";
SELECT * FROM Lavoratore WHERE stipendio BETWEEN 1800 AND 2000;
SELECT * FROM Lavoratore WHERE stipendio BETWEEN 2000 AND 2500 AND cognome LIKE "%i";
SELECT nome, cognome FROM Lavoratore WHERE stipendio BETWEEN 1900 AND 2400 ORDER BY stipendio DESC;
SELECT nome, cognome FROM Lavoratore WHERE stipendio BETWEEN 2000 AND 2500 ORDER BY stipendio DESC, nome ASC;
INSERT INTO Automobile VALUES ("Fiat", NULL, NULL, 1233, NULL);
UPDATE Automobile SET modello="500L", targa="XX123YY", colore="Bianco" WHERE marca="Fiat";
DELETE FROM Automobile WHERE marca LIKE "F%";


