/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modulo.ofertas.dto;

/**
 *
 * @author User
 */
public class PresentacionDto {

    private int idPresentacion = 0;
    private String descripcion = "";

    @Override
    public String toString() {
        return "PresentacionDto{" + "idPresentacion=" + idPresentacion + ", descripcion=" + descripcion + '}';
    }

    /**
     * @return the idPresentacion
     */
    public int getIdPresentacion() {
        return idPresentacion;
    }

    /**
     * @param idPresentacion the idPresentacion to set
     */
    public void setIdPresentacion(int idPresentacion) {
        this.idPresentacion = idPresentacion;
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
