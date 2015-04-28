<%-- 
    Document   : CargarPromociones
    Created on : 2/04/2015, 11:59:04 AM
    Author     : User
--%>

<%@page import="modulo.ofertas.dto.PresentacionDto"%>
<%@page import="modulo.ofertas.dto.PromocionDto"%>
<%@page import="java.util.ArrayList"%>
<%@page import="modulo.ofertas.dto.ProductoDto"%>
<%@page import="modulo.ofertas.dto.OfertaDto"%>
<%@page import="modulo.ofertas.FOferta"%>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        
        
        <title>JSP Page</title>
    </head>
    <body>
        <%

            ProductoDto pDto;

            FOferta faOfer = new FOferta();

            int idProductoAso = Integer.parseInt(request.getParameter("idProdAso"));

            pDto = faOfer.obtenerProductoConIdProductoAso(idProductoAso);

            int salida = faOfer.obtenerNumeroOfertasConProducto(Integer.parseInt(request.getParameter("idProducto")),Long.parseLong(request.getParameter("idProductor")));
            if (salida > 0) {
        %> 
        <h4 class="text-center">Lo sentimos pero ya realizo una oferta con este producto</h4> 
        <div class="form-group" id="ofPrecio">   
            <input type="hidden"  id="PrecioVenta" value="">       
        </div>
        <%
        } else {
        %>
    <legend class="text-center"><strong><%= pDto.getNombres()%></strong> (<%= faOfer.obtenerNombreDeCategoriaPorId(pDto.getIdCategoria())%>)</legend>
    <br>
    <div class="form-group" id="ofPrecio">
        <label class="col-sm-4 control-label" for="PrecioVenta">Precio para la venta</label>
        <div class="col-sm-8">                     

            <input type="text" class="form-control " id="PrecioVenta" value=""
                   name="PrecioVenta" maxlength="9" onblur="validarPrecio()">                
        </div>
    </div>
    <br><br>
    <div class="form-group" id="ofPresentacion">
        <label for="unidad" class="col-lg-4 control-label">Unidad del producto</label>                                     
        <div class="col-lg-8">
            <select class="form-control" name="presentacion" id="presentacion" onblur="validarPresentacion(this)">

                <option value="" selected="true">[Selecione la Unidad]</option>

                <%
                    ArrayList<PresentacionDto> presentaciones = (ArrayList<PresentacionDto>) faOfer.obtenerPresentaciones(pDto.getIdCategoria());
                    for (PresentacionDto presentacion : presentaciones) {
                %>
                <option value="<%=presentacion.getIdPresentacion()%>"> <%=presentacion.getDescripcion()%> </option>
                <%
                    }
                %>

            </select>
        </div>
    </div>
    <br><br>
    <div class="form-group" id="ofCantidad">
        <label class="col-sm-4 control-label" for="cantidad">Cantidad a ofertar</label>
        <div class="col-sm-8">                        
            <input type="text" class="form-control " id="cantidad" value=""
                   name="cantidad" maxlength="9" onblur="validarCantidad(this)">
        </div>
    </div>
    <br><br>
    <div class="panel-group" id="accordion" role="tablist" aria-multiselectable="true">                                                
        <div class="panel panel-default">
            <div class="panel-heading" role="tab" id="headingThree">
                <h4 class="panel-title">
                    <a class="collapsed" data-toggle="collapse" data-parent="#accordion" href="#collapseThree" aria-expanded="false" aria-controls="collapseThree">
                        Agregar una promoción
                    </a>
                </h4>
            </div>
            <div id="collapseThree" class="panel-collapse collapse" role="tabpanel" aria-labelledby="headingThree">
                <div class="panel-body">
                    <div class="" id="ofPromocion">

                        <select name="promocion" id="promocion"  class="form-control" onblur="validarPromocion(this)">
                            <option value="1" selected="selected">[Seleccione una promocion]</option>
                            <%
                                ArrayList<PromocionDto> promociones = (ArrayList<PromocionDto>) faOfer.obtenerPromociones();
                                for (PromocionDto n : promociones) {
                            %>
                            <option value="<%=n.getIdPromocion()%>"><%=n.getDescripcion()%> </option>
                            <%
                                }

                            %>
                        </select>

                    </div>
                    <br>
                    <input type="hidden" name="idProductoAsociado" value="<%=idProductoAso%>">                                                            
                </div>
            </div>
        </div>
    </div>



    <%            }
    %>
</body>
</html>


