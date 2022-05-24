package org.dbsp.sqlCompiler.compiler;

import java.util.ArrayList;
import java.util.List;

public class CalciteProgram {
    public final List<ViewDDL> views;
    public final List<TableDDL> inputTables;

    public CalciteProgram() {
        this.views = new ArrayList<>();
        this.inputTables = new ArrayList<>();
    }

    public void addView(ViewDDL result) {
        this.views.add(result);
    }

    public void addInput(TableDDL table) {
        this.inputTables.add(table);
    }
}
