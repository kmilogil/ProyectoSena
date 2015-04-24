/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package modulo.pedidos.dto;

import modulo.ofertas.dto.OfertaDto;

/**
 *
 * @author User
 */
public class CarritoDto {
    private int idPedido = 0;
    private int idOferta = 0;
    private int cantidad = 0;
    private OfertaDto ofDto = new OfertaDto();
    private PedidoDto pedDto = new PedidoDto();

    @Override
    public String toString() {
        return "CarritoDto{" + "idPedido=" + getIdPedido() + ", idOferta=" + getIdOferta() + ", cantidad=" + getCantidad() + ", ofDto=" + getOfDto() + ", pedDto=" + getPedDto() + '}';
    }

   

    /**
     * @return the idPedido
     */
    public int getIdPedido() {
        return idPedido;
    }

    /**
     * @param idPedido the idPedido to set
     */
    public void setIdPedido(int idPedido) {
        this.idPedido = idPedido;
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
    public int getCantidad() {
        return cantidad;
    }

    /**
     * @param cantidad the cantidad to set
     */
    public void setCantidad(int cantidad) {
        this.cantidad = cantidad;
    }

    /**
     * @return the ofDto
     */
    public OfertaDto getOfDto() {
        return ofDto;
    }

    /**
     * @param ofDto the ofDto to set
     */
    public void setOfDto(OfertaDto ofDto) {
        this.ofDto = ofDto;
    }

    /**
     * @return the pedDto
     */
    public PedidoDto getPedDto() {
        return pedDto;
    }

    /**
     * @param pedDto the pedDto to set
     */
    public void setPedDto(PedidoDto pedDto) {
        this.pedDto = pedDto;
    }
}
