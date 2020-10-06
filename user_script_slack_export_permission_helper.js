// ==UserScript==
// @name     slack_export_permission_helper
// @version  2
// @grant    none
// @include https://api.slack.com/apps/*/oauth*
// @require https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js
// ==/UserScript==

var userbutton
var intervalID
var permissions = [
'channels:history',
'channels:read',
'files:read',
'groups:history',
'groups:read',
'im:history',
'im:read',
'mpim:history',
'mpim:read',
'users:read'
]
/*
Eventually this may be the way to do it when there's a method to populate the dropdown completely.
function clickAndCheckPermissions(){

  //Note I don't know how to manipulate the react code on the page to fully populate the dropdown
  //So I need some human help scolling the list to make it populate.
  //if you stop scrolling react removes elements not shown in the dropdown
  //
  // So helpful human - keep Chun Li spinning please?
  //
  clickablePermissions = $("div.p-app-scopes-picker__option").children('div').not('.p-app-scopes-picker__option-description')

  //console.log('see ' + divs.length.toString() + ' selectable permissions')

  clickablePermissions.each(function(){
      var txt = $(this).text()
      if (permissions.indexOf(txt) > -1){ //exact match in array
        //Ahuh! Give it to me.
        $(this).click()
        //After the click selection closes the dropdown, reopen it for more "spinning time kick".
        userbutton.delay(500).click()
        console.log('Acquired permission: ' + txt)
        //break out of this each loop - click one at a time, rinse repeat.
        return false;
      }
  });

  //Check what we have now.  eg <a class='c-link' href=/scopes/read:user ...>
  selectedPermissions = $('a.c-link[href^="/scopes/"]')

  //Start telling the console what we need incase we're getting unlucky with spin timing.
  if(selectedPermissions.length >= (permissions.length - 1)){
    //Deep copy
    var need = [].concat(permissions)
    //~ need = permissions - selected
    selectedPermissions.each(function(){
        var perm = $(this).attr('href').replace('/scopes/','')
        const index = need.indexOf(perm);
        if (index > -1) {
          need.splice(index, 1);
        }
    });
    if(need.length == 0){
      alert("I think you have all permissions, double check, and install, stopping clickAndCheckPermissions")
      clearInterval(intervalID)
    } else {
      console.log('still need ' + need.join(', '))
    }
  }

}
*/

function showPermissions(){
  //$("h2").filter(function(){
  //  return $(this).text() === "Scopes"; // Matches exact string
  //})[0].scrollIntoView()

  $("div.p-app-scopes-list-container__title").filter(function(){
    return $(this).text() === "User Token Scopes"; // Matches exact string
  })[0].scrollIntoView()
}

function addAllAndDelete(){
  clickablePermissions = $("div.p-app-scopes-picker__option").children('div').not('.p-app-scopes-picker__option-description')
  if (clickablePermissions.length == 0){
    console.log("Nothing left to add, start deleting.")
    clearInterval(intervalID)
    //Check what we have now.  eg <a class='c-link' href=/scopes/read:user ...>
    selectedPermissions = $('a.c-link[href^="/scopes/"]')
    console.log("selected")
    console.log(selectedPermissions.length)
    i=0
    selectedPermissions.each(function(){
      var perm = $(this).attr('href').replace('/scopes/','')

      const index = permissions.indexOf(perm);
      //If not in required permission list click the delete button.
      if (index == -1) {
        //console.log("Delete " + perm)
        $(this).parent().parent().find('button').click()
      }
      else{
        i+=1
        console.log(i.toString() + ") " + perm)
      }
    });
    showPermissions()
    return
  } else {
    //Add it whatever it is until there is nothing left to add.
    clickablePermissions.first().click()
  }
}

function addList() {
  //Check what we have now.  eg <a class='c-link' href=/scopes/read:user ...>
  selectedPermissions = $('a.c-link[href^="/scopes/"]')

  if(selectedPermissions.length == permissions.length){
    //Deep copy
    var need = [].concat(permissions)
    //~ need = permissions - selected
    selectedPermissions.each(function(){
        var perm = $(this).attr('href').replace('/scopes/','')
        const index = need.indexOf(perm);
        if (index > -1) {
          need.splice(index, 1);
        }
    });
    if(need.length == 0){
      console.log("I think you have all permissions, double check, and install")
      showPermissions()
      return
    }
  }
  console.log('This is a bit hockey, I do not know how to convince the dropdown to populate with all options. ' +
              'So instead select everything (grunt force populate) and then delete what we do not want')
  intervalID = setInterval(function(){
    userbutton.click()
    //Wait for changes
    setTimeout(addAllAndDelete, 250)
  }, 500);
}

function kickoff(){
  //second button (bot button is first)
      userbutton  = $("button").filter(function(){
      return $(this).text() === "Add an OAuth Scope"; // Matches exact string
  }).filter(':eq(1)');

  listButton =$('<button>Add slack-export.py permissions (User Token Scopes)</button>')
  listButton.attr('class', 'c-button c-button--outline c-button--small margin_right_50')
  listButton.click(addList)
  $('h1').before(listButton)

  //The preferred method, if a way is found to populate the dropdown and avoid human wheel spinning.
  //Every 1 second attempt to get the next needed permission.
  //userbutton.click()
  //intervalID = setInterval(clickAndCheckPermissions, 1000);
}

// ---- Main ----
$(document).ready(function() {
  //permissions take a while after first page load.
  setTimeout(kickoff, 3000)
});
