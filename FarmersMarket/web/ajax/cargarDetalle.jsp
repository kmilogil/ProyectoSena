<%-- 
    Document   : cargarDetalle
    Created on : 6/04/2015, 10:00:04 PM
    Author     : User
--%>

<%@page import="modulo.pedidos.dto.CarritoDto"%>
<%@page import="java.util.ArrayList"%>
<%@page import="modulo.pedidos.FPedido"%>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>JSP Page</title>
    </head>
    <body>
        <%
            FPedido faPedi = new FPedido();
            if (request.getParameter("idpc") != null) {
                ArrayList<CarritoDto> pedidos = (ArrayList<CarritoDto>) faPedi.obtenerDetallePedidosCliente(Integer.parseInt(request.getParameter("idpc")));

        %>

        <table class="table table-striped">
            <thead>
                <tr>
                    <th>Producto</th>
                    <th>Unidad</th>
                    <th>Cantidad</th>                                                    
                </tr>
            </thead>
            <tbody>

                <%                    for (CarritoDto pedido : pedidos) {
                %> 
                <tr>
                    <td><%=pedido.getOfDto().getProAso().getProDto().getNombres()%></td>
                    <td><%=pedido.getOfDto().getPreDto().getDescripcion()%></td>
                    <td><%=pedido.getCantidad()%></td>                                                    
                </tr>               
                <%
                    }
                %>
            </tbody>
        </table> 
        <%
        } else if (request.getParameter("idpp") != null) {
            ArrayList<CarritoDto> pedidos = (ArrayList<CarritoDto>) faPedi.obtenerDetallePedidosProductor(Integer.parseInt(request.getParameter("idpp")));

        %>
        <table class="table table-striped">
            <thead>
                <tr>
                    <th>Producto</th>
                    <th>Unidad</th>
                    <th>Cantidad</th>
                    <th>Direccion Residencia</th>
                    <th>Ciudad</th>
                </tr>
            </thead>
            <tbody>

                <%                    for (CarritoDto pedido : pedidos) {

                %> 
                <tr>
                    <td><%=pedido.getOfDto().getProAso().getProDto().getNombres()%></td>
                    <td><%=pedido.getOfDto().getPreDto().getDescripcion()%></td>
                    <td><%=pedido.getCantidad()%></td>
                    <td><%=pedido.getPedDto().getUsDto().getDireccion()%></td>
                    <td><%=pedido.getPedDto().getUsDto().getCiDto().getNombre()%></td>
                </tr>               
                <%                }
                %>
            </tbody>
        </table> 
        <%            
            }

        %>
    </body>
</html>
