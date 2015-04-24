package modulo.usuarios.dto;

/**
 * @author Kennit David Ruz Romero Hora de creación: 11:18 a.m. Fecha: 4 de
 * Marzo de 2015
 */
public class RolDto {

    private int idRol = 0;
    private String descripcion = "";

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

    /**
     * @return the descripcion
     */
    public String getDescripcion() {
        return descripcion;
    }

    /**
     * @param descripcion the descripcion to set
     */
    public void setDescripcion(String descripcion) {
        this.descripcion = descripcion;
    }

}
