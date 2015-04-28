/* 
 * Cargar Ciudades en Pos de un Departamento
 */

var xmlHttp;
function getSubcategorias(idDepartamento) {

    if (window.XMLHttpRequest) {
        xmlHttp = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
    } else {
        alert("El navegador no soporta Ajax!");
        return;
    }

    var url = "ajax/cargarCiudades.jsp?idDepartamento=" + idDepartamento;
    xmlHttp.onreadystatechange = resultadoCiudades;
    xmlHttp.open("GET", url, true);
    xmlHttp.send(null);

}

function resultadoCiudades() {
    if (xmlHttp.readyState === 4) {
        document.getElementById("ruCiudad").innerHTML = xmlHttp.responseText;
    }
}
/* 
 *  Fin de cargar Ciudades en Pos de un Departamento
 */

/* 
 * Cargar Permisos en Pos de un Rol
 */

var xmlHttp;
function getPermisos(idRol) {

    if (window.XMLHttpRequest) {
        xmlHttp = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
    } else {
        alert("El navegador no soporta Ajax!");
        return;
    }

    var url = "../ajax/cargarPermisos.jsp?idRol=" + idRol;
    xmlHttp.onreadystatechange = resultadoPermisos;
    xmlHttp.open("GET", url, true);
    xmlHttp.send(null);

}

function resultadoPermisos() {
    if (xmlHttp.readyState === 4) {
        document.getElementById("sesionPermisos").innerHTML = xmlHttp.responseText;
    }
}
/* 
 *  Fin de cargar Permisos en Pos de un Rol
 */

/* 
 * Cargar Formulario de Ofertar Producto
 */

var xmlHttp;
function getFormOfertar(idProducoAso) {

    if (window.XMLHttpRequest) {
        xmlHttp = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
    } else {
        alert("El navegador no soporta Ajax!");
        return;
    }

    var url = "../ajax/cargarPublicarOferta.jsp?idProducoAso=" + idProducoAso;
    xmlHttp.onreadystatechange = resultadoFormOfertar;
    xmlHttp.open("GET", url, true);
    xmlHttp.send(null);

}

function resultadoFormOfertar() {
    if (xmlHttp.readyState === 4) {
        document.getElementById("formularioPublicarOferta").innerHTML = xmlHttp.responseText;
    }
}
/* 
 *  Fin de cargar ofertar
 */


     

// Realizar Pedido
var xmlHttp;
function getPedido(idOferta) {
    
    if (window.XMLHttpRequest) {
        xmlHttp = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
    } else {
        alert("El navegador no soporta Ajax!");
        return;
    }

    var url = "../ajax/cargarPedido.jsp?idOferta=" + idOferta;
    xmlHttp.onreadystatechange = resultadoPedido;
    xmlHttp.open("GET", url, true);
    xmlHttp.send(null);

}

function resultadoPedido() {
    if (xmlHttp.readyState === 4) {
        document.getElementById("formularioRealizarPedido").innerHTML = xmlHttp.responseText;
    }
}

var xmlHttp;
function getNumeroOfertas(idProducto, idProdAso, idProductor) {

    if (window.XMLHttpRequest) {
        xmlHttp = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
    } else {
        alert("El navegador no soporta Ajax!");
        return;
    }

    var url = "../ajax/validarOferta.jsp?idProducto=" + idProducto + "&idProdAso=" + idProdAso + "&idProductor=" + idProductor;
    xmlHttp.onreadystatechange = resultadonumeroOferta;
    xmlHttp.open("GET", url, true);
    xmlHttp.send(null);

}

function resultadonumeroOferta() {
    if (xmlHttp.readyState === 4) {
        if (xmlHttp.responseText !== null) {
            document.getElementById("modalOferta").innerHTML = xmlHttp.responseText;
            
        }

    }
}

var xmlHttp;
function getAbastecimiento(idOferta) {
    
    if (window.XMLHttpRequest) {
        xmlHttp = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
    } else {
        alert("El navegador no soporta Ajax!");
        return;
    }

    var url = "../ajax/cargarPedido.jsp?idOfer=" + idOferta;
    xmlHttp.onreadystatechange = resultadoAbastecer;
    xmlHttp.open("GET", url, true);
    xmlHttp.send(null);

}

function resultadoAbastecer() {
    if (xmlHttp.readyState === 4) {
        document.getElementById("formAbastecimiento").innerHTML = xmlHttp.responseText;
    }
}


var xmlHttp;
function getDetalleCliente(idPedido) {
    
    if (window.XMLHttpRequest) {
        xmlHttp = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
    } else {
        alert("El navegador no soporta Ajax!");
        return;
    }

    var url = "../ajax/cargarDetalle.jsp?idpc=" + idPedido;
    xmlHttp.onreadystatechange = resultadoDetalleCliente;
    xmlHttp.open("GET", url, true);
    xmlHttp.send(null);

}

function resultadoDetalleCliente() {
    if (xmlHttp.readyState === 4) {
        document.getElementById("tablaDetalleCliente").innerHTML = xmlHttp.responseText;
    }
}

var xmlHttp;
function getDetalleProductor(idPedido) {
    
    if (window.XMLHttpRequest) {
        xmlHttp = new XMLHttpRequest();
    }
    else if (window.ActiveXObject) {
        xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
    } else {
        alert("El navegador no soporta Ajax!");
        return;
    }

    var url = "../ajax/cargarDetalle.jsp?idpp=" + idPedido;
    xmlHttp.onreadystatechange = resultadoDetalleProductor;
    xmlHttp.open("GET", url, true);
    xmlHttp.send(null);

}

function resultadoDetalleProductor() {
    if (xmlHttp.readyState === 4) {
        document.getElementById("tablaDetalleProductor").innerHTML = xmlHttp.responseText;
    }
}