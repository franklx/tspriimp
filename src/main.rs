use std::collections::HashSet;
use std::fs::remove_file;
use std::path::PathBuf;

use rusqlite::Connection;
use xlsx_batch_reader::read::XlsxBook;

const CD_DITTA: usize = 0;
const RS_DITTA: usize = 1;
const TIPO: usize = 2;
const CODICE: usize = 3;
const RAGSOC: usize = 4;
///// INDIRIZZO: usize = 5;
///// CAP: usize = 6;
///// CITTA: usize = 7;
///// PROV: usize = 8;
const PARTITA_IVA: usize = 9;
const CD_FISCALE: usize = 10;

fn main() -> anyhow::Result<()> {
    let in_path: PathBuf =
        std::env::args().nth(1).unwrap_or_else(|| std::env::var("IN_PATH").expect("Missing IN_PATH")).into();
    let out_file: PathBuf =
        std::env::args().nth(2).unwrap_or_else(|| std::env::var("OUT_FILE").expect("Missing OUT_FILE")).into();
    let mut ditte: HashSet<String> = HashSet::new();
    remove_file(&out_file).ok();
    let mut db = Connection::open(&out_file)?;
    db.execute_batch(
        "
        CREATE TABLE ditte (
            ragsoc TEXT,
            ditta INTEGER,
            PRIMARY KEY (ragsoc)
        );
        CREATE TABLE anag (
            ditta INTEGER,
            tipo INTEGER,
            codice INTEGER,
            ragsoc TEXT,
            partiv TEXT,
            codfis TEXT,
            PRIMARY KEY (ditta, tipo, codice)
        );
    ",
    )?;
    for xf in ["PRICLI.xlsx", "PRIFOR.xlsx"] {
        println!("Processing {r:?}...", r = in_path.join(xf));
        let mut wb = XlsxBook::new(in_path.join(xf), true)?;
        for sn in wb.get_visible_sheets().clone() {
            let ws = wb.get_sheet_by_name(&sn, 5000, 0, 1, 11, true)?;
            for batch in ws {
                let tx = db.transaction()?;
                if let Ok((_, rows)) = batch {
                    for rw in rows {
                        if let Some(ref dsc_ditta) = rw[RS_DITTA].get::<String>()? {
                            if !ditte.contains(dsc_ditta) {
                                tx.execute(
                                    "INSERT INTO ditte (ragsoc, ditta) VALUES (?1, ?2)",
                                    (dsc_ditta, rw[CD_DITTA].get::<i64>()?.unwrap_or_default()),
                                )?;
                                ditte.insert(dsc_ditta.to_string());
                            }
                        }
                        tx.execute("INSERT INTO anag (ditta, tipo, codice, ragsoc, partiv, codfis) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            (
                                rw[CD_DITTA].get::<i64>()?,
                                rw[TIPO].get::<i64>()?,
                                rw[CODICE].get::<i64>()?,
                                rw[RAGSOC].get::<String>()?,
                                rw[PARTITA_IVA].get::<String>()?,
                                rw[CD_FISCALE].get::<String>()?
                            )
                        )?;
                    }
                }
                tx.commit()?;
            }
        }
    }
    Ok(())
}
