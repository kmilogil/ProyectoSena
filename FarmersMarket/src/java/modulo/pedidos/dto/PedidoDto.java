/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modulo.pedidos.dto;

import modulo.usuarios.dto.UsuarioDto;

/**
 *
 * @author User
 */
public class PedidoDto {

    private int idPedido = 0;
    private long idCliente = 0;
    private String fechaPedido = "";
    private String fechaEntrega = "";
    private int estadoPedido = 0;
    private long total = 0;
    private UsuarioDto usDto = new UsuarioDto();

    @Override
    public String toString() {
        return "PedidoDto{" + "idPedido=" + idPedido + ", idCliente=" + idCliente + ", fechaPedido=" + fechaPedido + ", fechaEntrega=" + fechaEntrega + ", estadoPedido=" + estadoPedido + ", total=" + total + ", usDto=" + usDto + '}';
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
     * @return the idCliente
     */
    public long getIdCliente() {
        return idCliente;
    }

    /**
     * @param idCliente the idCliente to set
     */
    public void setIdCliente(long idCliente) {
        this.idCliente = idCliente;
    }

    /**
     * @return the fechaPedido
     */
    public String getFechaPedido() {
        return fechaPedido;
    }

    /**
     * @param fechaPedido the fechaPedido to set
     */
    public void setFechaPedido(String fechaPedido) {
        this.fechaPedido = fechaPedido;
    }

    /**
     * @return the fechaEntrega
     */
    public String getFechaEntrega() {
        return fechaEntrega;
    }

    /**
     * @param fechaEntrega the fechaEntrega to set
     */
    public void setFechaEntrega(String fechaEntrega) {
        this.fechaEntrega = fechaEntrega;
    }

    /**
     * @return the estadoPedido
     */
    public int getEstadoPedido() {
        return estadoPedido;
    }

    /**
     * @param estadoPedido the estadoPedido to set
     */
    public void setEstadoPedido(int estadoPedido) {
        this.estadoPedido = estadoPedido;
    }

    /**
     * @return the total
     */
    public long getTotal() {
        return total;
    }

    /**
     * @param total the total to set
     */
    public void setTotal(long total) {
        this.total = total;
    }

    /**
     * @return the usDto
     */
    public UsuarioDto getUsDto() {
        return usDto;
    }

    /**
     * @param usDto the usDto to set
     */
    public void setUsDto(UsuarioDto usDto) {
        this.usDto = usDto;
    }

}
