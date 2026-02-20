use sqlparser::ast;
fn check_create_table(ct: &ast::CreateTable) {
    let _ = &ct.engine;
    let _ = &ct.table_options;
}
