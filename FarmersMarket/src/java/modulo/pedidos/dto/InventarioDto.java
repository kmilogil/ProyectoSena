/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package modulo.pedidos.dto;

/**
 *
 * @author User
 */
public class InventarioDto {
    
    private int idOferta = 0;
    private long cantidad = 0;

    @Override
    public String toString() {
        return "InventarioDto{" + "idOferta=" + idOferta + ", cantidad=" + cantidad + '}';
    }
    

    

    /**
     * @return the idOferta
     */
    public int getIdOferta() {
        return idOferta;
    }

    /**
     * @param idOferta the idOferta to set
     */
    public void setIdOferta(int idOferta) {
        this.idOferta = idOferta;
    }

    /**
     * @return the cantidad
     */
    public long getCantidad() {
        return cantidad;
    }

    /**
     * @param cantidad the cantidad to set
     */
    public void setCantidad(long cantidad) {
        this.cantidad = cantidad;
    }

    
}
