package modulo.usuarios.dto;

/**
 * @author Kennit David Ruz Romero 
 * Fecha: 4 de de Enero de 2015
 */

public class PermisoRolDto {
    
    private int idPermiso = 0;
    private int idRol = 0;

    @Override
    public String toString() {
        return "PermisoRolDto{" + "idPermiso=" + idPermiso + ", idRol=" + idRol + '}';
    }

    /**
     * @return the idPermiso
     */
    public int getIdPermiso() {
        return idPermiso;
    }

    /**
     * @param idPermiso the idPermiso to set
     */
    public void setIdPermiso(int idPermiso) {
        this.idPermiso = idPermiso;
    }

    /**
     * @return the idRol
     */
    public int getIdRol() {
        return idRol;
    }

    /**
     * @param idRol the idRol to set
     */
    public void setIdRol(int idRol) {
        this.idRol = idRol;
    }
}
