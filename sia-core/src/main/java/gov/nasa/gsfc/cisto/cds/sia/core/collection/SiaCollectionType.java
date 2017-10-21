package gov.nasa.gsfc.cisto.cds.sia.core.collection;


/**
 * The enum Sia collection type.
 */
public enum SiaCollectionType {

    /**
     * Sia collection parent sia collection type.
     */
    SIACollectionParent(null),
    /**
     * Merra sia collection type.
     */
    Merra(SIACollectionParent),
    /**
     * Independent variable type sia collection type.
     */
    IndependentVariableType(Merra),
    /**
     * Const 2 d asm nx sia collection type.
     */
    const_2d_asm_Nx(IndependentVariableType),
    /**
     * Const 2 d mld nx sia collection type.
     */
    const_2d_mld_Nx(IndependentVariableType),
    /**
     * Analysis collection type sia collection type.
     */
    AnalysisCollectionType(Merra),
    /**
     * Inst 6 3 d ana nv sia collection type.
     */
    inst6_3d_ana_Nv(AnalysisCollectionType),
    ;

    private SiaCollectionType parent = null;

    private SiaCollectionType(SiaCollectionType parent) {
        this.parent = parent;
    }
}
